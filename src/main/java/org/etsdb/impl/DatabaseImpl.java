package org.etsdb.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.etsdb.utils.atomic.NotifyAtomicInteger;
import org.dsa.iot.etsdb.utils.atomic.NotifyAtomicLong;
import org.etsdb.ByteArrayBuilder;
import org.etsdb.Database;
import org.etsdb.DbConfig;
import org.etsdb.EtsdbException;
import org.etsdb.QueryCallback;
import org.etsdb.Serializer;
import org.etsdb.TimeRange;
import org.etsdb.util.DirectoryUtils;
import org.etsdb.util.EventHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @param <T> the class of data that is written to this database.
 * @author Matthew Lohbihler
 */
public class DatabaseImpl<T> implements Database<T> {

    public static final int VERSION = 2;
    static final Logger logger = LoggerFactory.getLogger(DatabaseImpl.class.getName());
    final Serializer<T> serializer;
    int shardStalePeriod;
    // Open shards
    int maxOpenFiles;
    final NotifyAtomicInteger openShards = new NotifyAtomicInteger();
    final NotifyAtomicInteger openFiles = new NotifyAtomicInteger();
    // Write queue
    WriteQueueInfo queueInfo;
    final NotifyAtomicLong flushCount = new NotifyAtomicLong();
    final AtomicLong forcedClose = new AtomicLong();
    final NotifyAtomicLong flushForced = new NotifyAtomicLong();
    final NotifyAtomicLong flushExpired = new NotifyAtomicLong();
    final NotifyAtomicLong flushLimit = new NotifyAtomicLong();
    // Configuration
    private File baseDir;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private Janitor janitor;
    private final Map<String, Series<T>> seriesLookup = new HashMap<>();
    // Backdates
    private Backdates backdates;
    // Monitors
    private final EventHistogram writesPerSecond = new EventHistogram(5000, 2);
    private final NotifyAtomicLong writeCount = new NotifyAtomicLong();
    private final NotifyAtomicLong backdateCount = new NotifyAtomicLong();
    // Runtime
    private final DbConfig config;
    private boolean closed;

    public DatabaseImpl(File baseDir, Serializer<T> serializer, DbConfig config) {
        if (config == null) // Use the defaults.
        {
            config = new DbConfig();
        }
        config.validate();
        this.config = config;
        this.serializer = serializer;
        this.baseDir = baseDir;
        open();
    }

    public void move(File newLoc) {
        try {
            close();
        } catch (Exception ignored) {
        }
        lockExclusive();
        try {
            if (!this.baseDir.renameTo(newLoc)) {
                throw new RuntimeException("Failed to move database");
            }
            this.baseDir = newLoc;
        } catch (RuntimeException e) {
            try {
                close();
            } catch (Exception ignored) {
            }
        } finally {
            open();
            unlockExclusive();
        }
    }

    private void open() {
        closed = false;
        if (!baseDir.exists()) {
            if (!baseDir.mkdirs()) {
                logger.error("Failed to create baseDir: {}", baseDir.getParent());
            }
        }

        logger.info("Database started at {}", baseDir.getAbsolutePath());

        shardStalePeriod = config.getShardStalePeriod();
        if (config.isIgnoreBackdates()) {
            backdates = null;
        } else {
            backdates = new Backdates(this, config.getBackdateStartDelay());
        }

        queueInfo = config.isUseWriteQueue() ? new WriteQueueInfo(config) : null;

        janitor = new Janitor(this);
        janitor.lock();
        janitor.setFileLockCheckInterval(config.getFileLockCheckInterval());
        janitor.setFlushInterval(config.getFlushInterval());

        if (config.isDeleteEmptyDirs()) {
            // Clean up the file structure.
            long start = System.currentTimeMillis();
            Utils.deleteEmptyDirs(baseDir);
            logger.info("Empty dir delete took " + (System.currentTimeMillis() - start) + "ms");
        }

        DBProperties props = getProperties();
        if (!props.getBoolean("clean", false)) {
            if (config.isRunCorruptionScan()) {
                long start = System.currentTimeMillis();
                new CorruptionScanner(this).scan();
                logger.info("Corruption scan took " + (System.currentTimeMillis() - start) + "ms");
            }
        } else {
            if (config.isRunCorruptionScan()) {
                logger.info("Corruption scan skipped because the database is marked as clean");
            }
            props.setBoolean("clean", false);
        }

        if (config.isAddShutdownHook()) {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        close();
                    } catch (IOException e) {
                        logger.warn("Exception during close", e);
                    }
                }
            });
        }

        maxOpenFiles = config.getMaxOpenFiles();

        janitor.initiate();
    }

    @Override
    public File getBaseDir() {
        return baseDir;
    }

    private void lockConcurrent() {
        lock.readLock().lock();
    }

    private void unlockConcurrent() {
        lock.readLock().unlock();
    }

    private void lockExclusive() {
        lock.writeLock().lock();
    }

    private void unlockExclusive() {
        lock.writeLock().unlock();
    }

    public void renameSeries(File oldDir, String toId) {
        lockConcurrent();
        try {
            File newDir = Utils.getSeriesDir(baseDir, toId);

            if (!oldDir.exists()) // Old series name doesn't exist. Nothing to do.
            {
                return;
            }

            if (newDir.equals(oldDir)) // Same file. Nothing to do.
            {
                return;
            }

            if (newDir.exists()) {
                throw new EtsdbException("New series name already exists");
            }

            if (!newDir.getParentFile().mkdirs()) {
                String dir = newDir.getParent();
                logger.error("Failed to create directory: {}", dir);
            }
            try {
                Utils.renameWithRetry(oldDir, newDir);
            } catch (IOException e) {
                try {
                    Utils.deleteWithRetry(newDir);
                } catch (IOException e1) {
                    // Ignore
                }
                throw new EtsdbException(e);
            }
        } finally {
            unlockConcurrent();
        }
    }

    boolean tooManyFiles() {
        return maxOpenFiles > 0 && openFiles.get() >= maxOpenFiles;
    }

    @Override
    public void write(String seriesId, long ts, T value) {
        lockConcurrent();
        try {
            writesPerSecond.hit();
            writeCount.incrementAndGet();
            try {
                // Lock for read, because the write actually occurs at the shard, not the series. I.e. we can permit
                // concurrent writes in a series.
                Series<T> series = getSeries(seriesId);
                series.write(ts, value);
            } catch (IOException e) {
                throw new EtsdbException(e);
            }
        } finally {
            unlockConcurrent();
        }
    }

    @Override
    public void query(String seriesId, long fromTs, long toTs, final QueryCallback<T> cb) {
        query(seriesId, fromTs, toTs, Integer.MAX_VALUE, false, cb);
    }

    @Override
    public void query(String seriesId, long fromTs, long toTs, int limit,
            final QueryCallback<T> cb) {
        query(seriesId, fromTs, toTs, limit, false, cb);
    }

    @Override
    public void query(String seriesId, long fromTs, long toTs, int limit, boolean reverse,
            final QueryCallback<T> cb) {
        lockConcurrent();
        try {
            Series<T> series = getSeries(seriesId);
            series.query(fromTs, toTs, limit, reverse, new CallbackWrapper(cb));
        } catch (IOException e) {
            throw new EtsdbException(e);
        } finally {
            unlockConcurrent();
        }
    }

    @Override
    public long count(String seriesId, long fromTs, long toTs) {
        lockConcurrent();
        try {
            final AtomicLong count = new AtomicLong();
            Series<T> series = getSeries(seriesId);
            series.query(fromTs, toTs, Integer.MAX_VALUE, false, new RawQueryCallback() {
                @Override
                public void sample(String seriesId, long ts, ByteArrayBuilder b) {
                    count.incrementAndGet();
                }
            });
            return count.get();
        } catch (IOException e) {
            throw new EtsdbException(e);
        } finally {
            unlockConcurrent();
        }
    }

    @Override
    public List<String> getSeriesIds() {
        lockConcurrent();
        try {
            List<String> ids = new ArrayList<>();

            File[] base = baseDir.listFiles();
            if (base != null) {
                for (File sub : base) {
                    if (sub.isDirectory()) {
                        int subPos = sub.getPath().length() + 1;
                        ids.addAll(list(sub, subPos));
                    }
                }
            }

            Collections.sort(ids);
            return ids;
        } finally {
            unlockConcurrent();
        }
    }

    private Set<String> list(File start, int subPos) {
        Set<String> filesList = new HashSet<>();
        File[] files = start.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    filesList.addAll(list(f, subPos));
                } else {
                    String name = f.getName();
                    if (name.endsWith(".data")) {
                        filesList.add(f.getParent().substring(subPos));
                    }
                }
            }
        }
        return filesList;
    }

    @Override
    public long getDatabaseSize() {
        return DirectoryUtils.getSize(baseDir).getSize();
    }

    @Override
    public long availableSpace() {
        return baseDir.getUsableSpace();
    }

    @Override
    public TimeRange getTimeRange(List<String> seriesIds) {
        lockConcurrent();
        try {
            TimeRange range = null;
            for (String id : seriesIds) {
                Series<T> series = getSeries(id);
                TimeRange tr = series.getTimeRange();
                if (range == null) {
                    range = tr;
                } else if (tr != null) {
                    range = range.union(tr);
                }
            }
            return range;
        } catch (IOException e) {
            throw new EtsdbException(e);
        } finally {
            unlockConcurrent();
        }
    }

    @Override
    public long delete(String seriesId, long fromTs, long toTs) {
        lockConcurrent();
        try {
            Series<T> series = getSeries(seriesId);
            return series.delete(fromTs, toTs);
        } catch (IOException e) {
            throw new EtsdbException(e);
        } finally {
            unlockConcurrent();
        }
    }

    @Override
    public void purge(String seriesId, long toTs) {
        lockConcurrent();
        try {
            Series<T> series = getSeries(seriesId);
            series.purge(toTs);
        } catch (IOException e) {
            throw new EtsdbException(e);
        } finally {
            unlockConcurrent();
        }
    }

    /**
     * @param seriesId ID to remove
     */
    @Override
    public void deleteSeries(String seriesId) {
        lockExclusive();
        try {
            synchronized (seriesLookup) {
                purge(seriesId, Long.MAX_VALUE);

                File seriesDir = Utils.getSeriesDir(baseDir, seriesId);
                try {
                    Utils.delete(seriesDir);
                } catch (IOException e) {
                    logger.warn("Error while deleting series " + seriesId, e);
                }
            }
        } finally {
            unlockExclusive();
        }
    }

    @Override
    @SuppressFBWarnings("DM_GC")
    public void close() throws IOException {
        lockExclusive();
        try {
            if (!closed) {
                if (backdates != null) {
                    try {
                        backdates.close();
                    } catch (Exception ignored) {
                    }
                }

                closed = true;

                janitor.terminate();
                janitor.join();

                flush(true);

                for (Series<T> series : getSerieses()) {
                    series.close();
                }

                System.gc();

                // Write a clean indicator into the database properties, so
                // that we know a corruption check isn't necessary upon next
                // start.
                getProperties().setBoolean("clean", true);
            }
        } finally {
            unlockExclusive();
        }
    }

    private Series<T> getSeries(String seriesId) throws IOException {
        if (closed) {
            throw new IOException("Database is closed");
        }

        seriesId = sanitizeSeriesId(seriesId);
        Series<T> series = seriesLookup.get(seriesId);
        if (series == null) {
            synchronized (seriesLookup) {
                series = seriesLookup.get(seriesId);
                if (series == null) {
                    series = new Series<>(this, baseDir, seriesId, serializer);
                    seriesLookup.put(seriesId, series);
                }
            }
        }
        return series;
    }

    private List<Series<T>> getSerieses() {
        // serieses: plural for series my precious
        List<Series<T>> serieses = new ArrayList<>();
        synchronized (seriesLookup) {
            serieses.addAll(seriesLookup.values());
        }
        return serieses;
    }

    public DBProperties getProperties() {
        return new DBProperties(this);
    }

    //
    //
    // Backdates
    //
    void addBackdate(Backdate backdate) {
        if (backdates != null) {
            backdateCount.incrementAndGet();
            backdates.add(backdate);
        }
    }

    void insert(String seriesId, long shardId, List<Backdate> backdates) {
        lockConcurrent();
        try {
            Series<T> series = getSeries(seriesId);
            series.insert(shardId, backdates);
        } catch (IOException e) {
            throw new EtsdbException(e);
        } finally {
            unlockConcurrent();
        }
    }

    //
    //
    // Write queue
    //
    boolean useQueue() {
        return queueInfo != null;
    }

    public int flush(boolean force) throws IOException {
        lockConcurrent();
        try {
            int closures = 0;

            long runtime = System.currentTimeMillis();
            List<Series<T>> serieses = getSerieses();
            for (Series<T> series : serieses) {
                closures += series.flush(runtime, force);
            }

            // If the size of the queue still exceeds the max size, start force flushing random series until it doesn't.
            if (useQueue()) {
                if (queueInfo.queueSize.get() > queueInfo.maxQueueSize) {
                    logger.info("Max queue size exceeded. Writing lists to reduce.");
                    while (!serieses.isEmpty()) {
                        if (queueInfo.queueSize.get() <= queueInfo.maxQueueSize) {
                            break;
                        }

                        int index = queueInfo.random.nextInt(serieses.size());
                        Series<T> series = serieses.remove(index);
                        closures += series.flush(runtime, true);
                    }
                }

                int discards = queueInfo.recentDiscards.getAndSet(0);
                if (discards > 0) {
                    logger.warn("Discarded " + discards + " writes");
                }
            }

            return closures;
        } finally {
            unlockConcurrent();
        }
    }

    //
    //
    // Monitors
    //
    @Override
    public int getWritesPerSecond() {
        return writesPerSecond.getEventCounts()[0] / 5;
    }

    @Override
    public long getWriteCount() {
        return writeCount.get();
    }

    public void setWriteCount(long count) {
        writeCount.set(count);
    }

    @Override
    public void setWriteCountHandler(Handler<Long> handler) {
        writeCount.setHandler(handler);
    }

    @Override
    public long getFlushCount() {
        return flushCount.get();
    }

    public void setFlushCount(long val) {
        flushCount.set(val);
    }

    @Override
    public void setFlushCountHandler(Handler<Long> handler) {
        flushCount.setHandler(handler);
    }

    @Override
    public long getBackdateCount() {
        return backdateCount.get();
    }

    public void setBackdateCount(long val) {
        backdateCount.set(val);
    }

    @Override
    public void setBackdateCountHandler(Handler<Long> handler) {
        backdateCount.setHandler(handler);
    }

    @Override
    public int getOpenFiles() {
        return openFiles.get();
    }

    @Override
    public void setOpenFilesHandler(Handler<Integer> handler) {
        openFiles.setHandler(handler);
    }

    @Override
    public long getFlushForced() {
        return flushForced.get();
    }

    public void setFlushForced(long val) {
        flushForced.set(val);
    }

    @Override
    public void setFlushForcedHandler(Handler<Long> handler) {
        flushForced.setHandler(handler);
    }

    @Override
    public long getFlushExpired() {
        return flushExpired.get();
    }

    public void setFlushExpired(long val) {
        flushExpired.set(val);
    }

    @Override
    public void setFlushExpiredHandler(Handler<Long> handler) {
        flushExpired.setHandler(handler);
    }

    @Override
    public long getFlushLimit() {
        return flushLimit.get();
    }

    public void setFlushLimit(long val) {
        flushLimit.set(val);
    }

    @Override
    public void setFlushLimitHandler(Handler<Long> handler) {
        flushLimit.setHandler(handler);
    }

    @Override
    public long getForcedClose() {
        return forcedClose.get();
    }

    @Override
    public int getLastFlushMillis() {
        return janitor.lastFlushMillis;
    }

    @Override
    public void setLastFlushMillisHandler(Handler<Integer> handler) {
        janitor.setFlushTimeHandler(handler);
    }

    @Override
    public int getQueueSize() {
        if (queueInfo == null) {
            return 0;
        }
        return queueInfo.queueSize.get();
    }

    @Override
    public void setQueueSizeHandler(Handler<Integer> handler) {
        if (queueInfo != null) {
            queueInfo.queueSize.setHandler(handler);
        }
    }

    @Override
    public int getOpenShards() {
        return openShards.get();
    }

    @Override
    public void setOpenShardsHandler(Handler<Integer> handler) {
        openShards.setHandler(handler);
    }

    private String sanitizeSeriesId(String seriesId) {
        if (seriesId.startsWith("/")) {
            return seriesId.substring(1);
        }
        return seriesId;
    }

    class CallbackWrapper implements RawQueryCallback {

        private final QueryCallback<T> cb;

        public CallbackWrapper(QueryCallback<T> cb) {
            this.cb = cb;
        }

        @Override
        public void sample(String seriesId, long ts, ByteArrayBuilder b) {
            T t = serializer.fromByteArray(b, ts);
            if (t != null) {
                cb.sample(seriesId, ts, t);
            }
        }
    }
}
