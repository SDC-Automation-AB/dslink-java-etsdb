package org.etsdb.impl;

import org.etsdb.ByteArrayBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class DataShard {

    private static final Logger logger = LoggerFactory.getLogger(DataShard.class.getName());

    private final DatabaseImpl<?> db;
    private final String seriesId;
    private final long shardId;
    private final File dataFile;
    private final File metaFile;

    private final PendingWriteList cache;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicInteger metaClosures = new AtomicInteger();
    /**
     * This field is the latest time in the data file *only*. There may be cache records with a later ts that this value does not represent.
     */
    private long latestTime = -Long.MAX_VALUE;
    private MappedByteBuffer metaBuf;
    private ChecksumOutputStream dataOut;
    private long lastAccess;
    private boolean closed;

    DataShard(DatabaseImpl<?> db, File seriesDir, String seriesId, long shardId) throws IOException {
        this.db = db;
        this.seriesId = seriesId;
        this.shardId = shardId;
        metaFile = new File(seriesDir, shardId + ".meta");
        dataFile = new File(seriesDir, shardId + ".data");

        if (dataFile.exists() && !metaFile.exists()) {
            recreateMetaFile();
        }

        cache = db.useQueue() ? new PendingWriteList(db.queueInfo) : null;

        updateLastAccess();
    }

    static void _writeSample(ChecksumOutputStream out, long tsOffset, byte[] data, int offset, int length)
            throws IOException {
        out.write(Utils.SAMPLE_HEADER);
        Utils.write4ByteUnsigned(out, tsOffset);
        Utils.writeCompactInt(out, length);
        out.write(data, offset, length);
        out.writeSum();
    }

    static void _readSample(ChecksumInput in, ScanInfo scanInfo) throws IOException {
        if (scanInfo.isEof()) {
            // If we're done with the file, start iterating through the cache.
            scanInfo.incrementCache();
            return;
        }

        int b = in.read();
        if (b == -1) {
            // EOF
            scanInfo.setEof(true);
            return;
        }

        // Header
        if (((byte) b) != Utils.SAMPLE_HEADER[0]) {
            throw new BadRowException("Header error at 0: expected " + Utils.SAMPLE_HEADER[0] + ", got " + b);
        }
        for (int i = 1; i < Utils.SAMPLE_HEADER.length; i++) {
            b = in.read();
            if (((byte) b) != Utils.SAMPLE_HEADER[i]) {
                throw new BadRowException("Header error at " + i + ": expected " + Utils.SAMPLE_HEADER[i] + ", got "
                        + b);
            }
        }

        // Offset
        try {
            scanInfo.setOffset(Utils.read4ByteUnsigned(in));
        } catch (IOException e) {
            throw new BadRowException("Offset error: IOException: " + e.getMessage());
        }

        // Length
        int length;
        try {
            length = Utils.readCompactInt(in);
        } catch (IOException e) {
            throw new BadRowException("Length error: IOException: " + e.getMessage());
        }
        if (length < 0 || length > Utils.MAX_DATA_LENGTH) {
            throw new BadRowException("Length error: cannot be negative or exceed " + Utils.MAX_DATA_LENGTH + ": "
                    + length);
        }

        // Data
        scanInfo.getData().clear();
        scanInfo.getData().put(in, length);

        if (in.isEof()) {
            throw new BadRowException("EOF before row was completely read");
        }
    }

    long getShardId() {
        return shardId;
    }

    void lockRead() {
        lock.readLock().lock();
    }

    void unlockRead() {
        lock.readLock().unlock();
    }

    void lockWrite() {
        lock.writeLock().lock();
    }

    void unlockWrite() {
        lock.writeLock().unlock();
    }

    boolean isClosed() {
        return closed;
    }

    List<PendingWrite> getCache() {
        if (cache == null) {
            return null;
        }
        return cache.getList();
    }

    int resetMetaClosures() {
        return metaClosures.getAndSet(0);
    }

    void write(long ts, byte[] data, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("DataShard already closed");
        }

        try {
            ensureLatestTime();

            long offset = Utils.getSampleOffset(ts);
            if (ts >= latestTime) {
                // Append
                if (cache == null) {
                    writeImmediate(ts, offset, data, off, len);
                    db.flushCount.incrementAndGet();
                    dataOut.flush();
                } else {
                    // First check if there are too many queued rows.
                    if (db.queueInfo.queueSize.incrementAndGet() > db.queueInfo.discardQueueSize) {
                        db.queueInfo.recentDiscards.incrementAndGet();
                        db.queueInfo.queueSize.decrementAndGet();
                    } else {
                        cache.add(new PendingWrite(offset, data, off, len));
                    }
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Backdate: seriesId=" + seriesId + ", ts=" + ts + ", latestTime=" + latestTime);
                }
                db.addBackdate(new Backdate(seriesId, shardId, offset, data, off, len));
            }
        } finally {
            updateLastAccess();
        }
    }

    private void writeImmediate(long ts, long offset, byte[] data, int off, int len) throws IOException {
        openData();
        openMeta();

        _writeSample(dataOut, offset, data, off, len);
        latestTime = ts;
        metaBuf.putLong(latestTime);
        metaBuf.flip();
    }

    int query(long fromOffset, long toOffset, int limit, RawQueryCallback cb) throws IOException {
        if (closed) {
            throw new IOException("DataShard already closed");
        }

        ChecksumInputStream in = null;
        int count = 0;
        try {
            ScanInfo scanInfo = new ScanInfo(getCache());
            in = new ChecksumInputStream(dataFile);

            while (count < limit) {
                readSample(in, scanInfo);
                if (scanInfo.isEndOfShard()) {
                    break;
                }

                if (scanInfo.getOffset() < fromOffset)
                    continue; // Ignore. Before time range
                else if (scanInfo.getOffset() >= toOffset) {
                    break; // After time range. Done.
                }
                cb.sample(seriesId, Utils.getTimestamp(shardId, scanInfo.getOffset()), scanInfo.getData());
                count++;
            }
        } finally {
            Utils.closeQuietly(in);
            updateLastAccess();
        }

        return count;
    }

    int queryReverse(long fromOffset, long toOffset, int limit, RawQueryCallback cb) throws IOException {
        if (closed) {
            throw new IOException("DataShard already closed");
        }

        int count = 0;
        try {
            ScanInfo scanInfo = new ScanInfo();

            // Check the cache for eligible rows first.
            if (cache != null) {
                PendingWrite p;
                for (int i = cache.getList().size() - 1; i >= 0; i--) {
                    p = cache.getList().get(i);

                    if (p.getOffset() >= toOffset)
                        continue; // Ignore. After time range.
                    else if (p.getOffset() < fromOffset) {
                        // Before time range. Because cache rows are always after the file rows, we know that there
                        // will be nothing of interest in the file. To prevent a file read, set the limit to 0.
                        limit = 0;
                        break;
                    }
                    // Found a cache row of interest. Use the scan info's builder in the callback.
                    scanInfo.getData().clear();
                    scanInfo.getData().put(p.getData());
                    cb.sample(seriesId, Utils.getTimestamp(shardId, p.getOffset()), scanInfo.getData());
                    count++;
                }
            }

            // Check if we need to look at the file.
            if (count < limit) {
                // Yup. Read the file.
                PositionQueue positions;
                if (limit == Integer.MAX_VALUE) {
                    positions = new PositionQueue(limit);
                } else {
                    positions = new PositionQueue(limit - count);
                }

                // Gather the positions of records in the time range in the shard.
                ChecksumInputStream in = null;
                try {
                    in = new ChecksumInputStream(dataFile);
                    while (true) {
                        long position = in.position();
                        readSample(in, scanInfo);

                        if (scanInfo.isEof()) {
                            break;
                        }

                        if (scanInfo.getOffset() < fromOffset)
                            continue; // Ignore. Before time range
                        else if (scanInfo.getOffset() >= toOffset) {
                            break; // After time range. Done.
                        }
                        positions.push(position);
                    }
                } finally {
                    Utils.closeQuietly(in);
                }

                // Use the found positions to retrieve the records in reverse.
                if (positions.size() > 0) {
                    RandomAccessFile raf = null;
                    scanInfo.reset();
                    try {
                        raf = new RandomAccessFile(dataFile, "r");
                        ChecksumDataInput craf = new ChecksumDataInput(raf);
                        for (int i = positions.size() - 1; i >= 0; i--) {
                            long position = positions.peek(i);
                            raf.seek(position);
                            readSample(craf, scanInfo);
                            cb.sample(seriesId, Utils.getTimestamp(shardId, scanInfo.getOffset()), scanInfo.getData());
                        }
                        count += positions.size();
                    } finally {
                        Utils.closeQuietly(raf);
                    }
                }
            }
        } finally {
            updateLastAccess();
        }

        return count;
    }

    long getMinTs() throws IOException {
        if (!dataFile.exists()) {
            if (cache == null || cache.isEmpty()) {
                return Long.MIN_VALUE;
            }
            return Utils.getTimestamp(shardId, cache.getList().get(0).getOffset());
        }

        ChecksumInputStream in = null;
        try {
            if (closed) {
                throw new IOException("DataShard already closed");
            }

            ScanInfo scanInfo = new ScanInfo();
            in = new ChecksumInputStream(dataFile);

            readSample(in, scanInfo);

            if (scanInfo.isEndOfShard()) {
                return Long.MAX_VALUE;
            }
            return Utils.getTimestamp(shardId, scanInfo.getOffset());
        } finally {
            Utils.closeQuietly(in);
            updateLastAccess();
        }
    }

    long getMaxTs() throws IOException {
        if (cache == null || cache.isEmpty()) {
            ensureLatestTime();
            return latestTime;
        }
        PendingWrite p = cache.getList().get(cache.getList().size() - 1);
        return Utils.getTimestamp(shardId, p.getOffset());
    }

    private void readSample(ChecksumInput in, ScanInfo scanInfo) throws IOException {
        _readSample(in, scanInfo);
        if (!scanInfo.isEof() && !in.checkSum()) {
            throw new IOException("Corruption detected in " + dataFile.getPath());
        }
    }

    long deleteSamples(long fromTs, long toTs) throws IOException {
        if (!dataFile.exists()) {
            return 0;
        }

        // Close the data output stream
        closeData();

        // Rewrite the file.
        File tempFile = getTempFile();
        ChecksumOutputStream tempOut = new ChecksumOutputStream(new FileOutputStream(tempFile, false));

        ChecksumInputStream in = null;
        ScanInfo scanInfo = new ScanInfo();
        long deleteCount = 0;
        try {
            in = new ChecksumInputStream(dataFile);
            ByteArrayBuilder b = scanInfo.getData();

            readSample(in, scanInfo);

            while (!scanInfo.isEof()) {
                long offset = scanInfo.getOffset();
                if (offset < fromTs || offset > toTs) {
                    _writeSample(tempOut, scanInfo.getOffset(), b.getBuffer(), b.getReadOffset(), b.getAvailable());
                } else {
                    deleteCount++;
                }
                readSample(in, scanInfo);
            }
        } finally {
            Utils.closeQuietly(in);
            Utils.closeQuietly(tempOut);
        }

        // Delete the old file and copy the temp to replace it.
        try {
            Utils.deleteWithRetry(dataFile);
        } finally {
            Utils.renameWithRetry(tempFile, dataFile);
        }
        return deleteCount;
    }

    /**
     * The list of backdates must be in chronological order.
     */
    void insertSamples(List<Backdate> backdates) throws IOException {
        if (!dataFile.exists()) {
            // This could happen if the shard was purged while the backdates were waiting to get written.
            for (Backdate backdate : backdates) {
                writeImmediate(Utils.getTimestamp(backdate.getShardId(), backdate.getOffset()), backdate.getOffset(),
                        backdate.getData(), 0, backdate.getData().length);
            }
            db.flushCount.addAndGet(backdates.size());
            dataOut.flush();
            return;
        }

        // Close the data output stream
        closeData();

        // Rewrite the file.
        File tempFile = getTempFile();
        ChecksumOutputStream tempOut = new ChecksumOutputStream(new FileOutputStream(tempFile, false));

        ChecksumInputStream in = null;
        ScanInfo scanInfo = new ScanInfo();
        try {
            in = new ChecksumInputStream(dataFile);
            ByteArrayBuilder b = scanInfo.getData();

            Iterator<Backdate> iter = backdates.iterator();
            Backdate next = iter.next();

            readSample(in, scanInfo);

            while (true) {
                if (scanInfo.isEof() && next == null) // All done.
                {
                    break;
                }

                if (next == null || scanInfo.getOffset() < next.getOffset()) {
                    // No more inserts, or the read sample is before the next insert. Write the current sample.
                    _writeSample(tempOut, scanInfo.getOffset(), b.getBuffer(), b.getReadOffset(), b.getAvailable());
                    readSample(in, scanInfo);
                } else if (scanInfo.isEof() || scanInfo.getOffset() > next.getOffset()) {
                    // No more samples, or the next is before the current. Write the next.
                    _writeSample(tempOut, next.getOffset(), next.getData(), 0, next.getData().length);
                    if (iter.hasNext()) {
                        next = iter.next();
                    } else {
                        next = null;
                    }
                } else if (scanInfo.getOffset() == next.getOffset()) {
                    // The sample and the next have the same timestamp. Overwrite with the next.
                    _writeSample(tempOut, next.getOffset(), next.getData(), 0, next.getData().length);
                    if (iter.hasNext()) {
                        next = iter.next();
                    } else {
                        next = null;
                    }
                    readSample(in, scanInfo);
                } else {
                    throw new RuntimeException("Unhandled condition");
                }
            }
        } finally {
            Utils.closeQuietly(in);
            Utils.closeQuietly(tempOut);
        }

        // Delete the old file and copy the temp to replace it.
        try {
            Utils.deleteWithRetry(dataFile);
        } finally {
            Utils.renameWithRetry(tempFile, dataFile);
        }
    }

    void close() {
        if (!closed) {
            closed = true;

            try {
                writeCache();
            } catch (IOException e) {
                logger.warn("Failed to write cache on close", e);
            }

            closeFiles();

            try {
                // Delete the temp file if it exists.
                Utils.delete(getTempFile());
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    void flush(long runtime, boolean force) throws IOException {
        if (cache != null) {
            if ((force && !cache.isEmpty()) || cache.expired(runtime) || cache.exceeds()) {
                if (force) {
                    db.flushForced.incrementAndGet();
                } else if (cache.expired(runtime)) {
                    db.flushExpired.incrementAndGet();
                } else {
                    db.flushLimit.incrementAndGet();
                }
                writeCache();
            }
        }

        if (lastAccess < runtime - db.shardStalePeriod && (cache == null || cache.isEmpty())) {
            close();
        }
    }

    private void writeCache() throws IOException {
        if (cache != null && !cache.isEmpty()) {
            for (PendingWrite p : cache.getList()) {
                writeImmediate(Utils.getTimestamp(shardId, p.getOffset()), p.getOffset(), p.getData(), 0,
                        p.getData().length);
            }
            dataOut.flush();
            db.queueInfo.queueSize.addAndGet(-cache.getList().size());
            db.flushCount.addAndGet(cache.getList().size());
            cache.clear();
            closeFiles();
        }
    }

    private void openData() throws IOException {
        if (dataOut == null) {
            if (!dataFile.getParentFile().exists()) {
                if (!dataFile.getParentFile().mkdirs()) {
                    String path = dataFile.getParent();
                    logger.error("Failed to create dataFile: {}", path);
                }
            }
            dataOut = new ChecksumOutputStream(new FileOutputStream(dataFile, dataFile.exists()));
            db.openFiles.incrementAndGet();
        }
    }

    private void openMeta() throws IOException {
        if (metaBuf == null) {
            RandomAccessFile raf = new RandomAccessFile(metaFile, "rw");
            metaBuf = raf.getChannel().map(MapMode.READ_WRITE, 0, 8);
            Utils.closeQuietly(raf);
            db.openFiles.incrementAndGet();
        }
    }

    void closeFiles() {
        closeData();
        closeMeta();
    }

    private void closeData() {
        if (dataOut != null) {
            Utils.closeQuietly(dataOut);
            dataOut = null;
            db.openFiles.decrementAndGet();
        }
    }

    private void closeMeta() {
        if (metaBuf != null) {
            // The file is actually closed when the buffer is GC'ed.
            metaBuf = null;
            metaClosures.incrementAndGet();
        }
    }

    //
    //
    // Private
    //
    private void updateLastAccess() {
        lastAccess = System.currentTimeMillis();
    }

    private File getTempFile() {
        return new File(dataFile.getParentFile(), dataFile.getName() + ".temp");
    }

    private void recreateMetaFile() throws IOException {
        final AtomicLong lastTs = new AtomicLong();
        query(0, Long.MAX_VALUE, Integer.MAX_VALUE, new RawQueryCallback() {
            @Override
            public void sample(String seriesId, long ts, ByteArrayBuilder b) {
                lastTs.set(ts);
            }
        });

        ByteArrayBuilder b = new ByteArrayBuilder(8);
        b.putLong(lastTs.get());

        FileOutputStream out = null;
        try {
            out = new FileOutputStream(metaFile);
            b.get(out, 8);
        } finally {
            Utils.closeQuietly(out);
        }
    }

    private void ensureLatestTime() throws IOException {
        // Get the latest time.
        if (latestTime == -Long.MAX_VALUE && metaFile.exists()) {
            try {
                openMeta();
                latestTime = metaBuf.getLong();
            } finally {
                closeMeta();
            }
        }
    }
}
