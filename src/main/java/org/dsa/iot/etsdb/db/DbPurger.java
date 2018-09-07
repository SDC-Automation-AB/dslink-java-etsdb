package org.dsa.iot.etsdb.db;

import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.etsdb.serializer.ByteData;
import org.etsdb.TimeRange;
import org.etsdb.impl.DatabaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Manages whether a database should be purged or not.
 *
 * @author Samuel Grenier
 */
public class DbPurger {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbPurger.class);
    private List<Db> databases = new ArrayList<>();
    private ScheduledFuture<?> fut;
    private boolean running;

    public synchronized void addDb(Db db) {
        if (!databases.contains(db)) {
            databases.add(db);
        }
    }

    public synchronized void removeDb(Db db) {
        databases.remove(db);
    }

    public void stop() {
        running = false;
        synchronized (this) {
            if (fut != null) {
                fut.cancel(true);
            }
        }
    }

    void setupPurger() {
        running = true;
        Runnable runner = new Runnable() {
            @Override
            public void run() {
                for (Db db : databases) {
                    if (!(db.isPurgeable() && running)) {
                        continue;
                    }

                    File path = db.getPath();
                    long curr = path.getUsableSpace();
                    long request = db.getDiskSpaceRemaining();
                    long delCount = 0;
                    if (curr - request <= 0) {
                        if (!running) {
                            break;
                        }
                        DatabaseImpl<ByteData> realDb = db.getDb();

                        List<String> series = realDb.getSeriesIds();
                        if (File.separatorChar != '/') {
                        	List<String> corrected = new ArrayList<String>();
	                        for (String s: series) {
	                        	corrected.add(s.replace(File.separatorChar, '/'));
	                        }
	                        series = corrected;
                        }
                        while (curr - request <= 0) {
	                        TimeRange range = realDb.getTimeRange(series);
	                        if (range == null || range.isUndefined()) {
	                            break;
	                        }
	
	                        long from = range.getFrom();
	                        for (String s : series) {
	                            delCount += realDb.delete(s, from, from + 3600000);
	                        }
	
	                        if (delCount <= 0) {
	                            break;
	                        }
	                        curr = path.getUsableSpace();
                        }
                    }
                    if (delCount > 0) {
                        String p = path.getPath();
                        LOGGER.info("Deleted {} records from {}", delCount, p);
                    }
                }
            }
        };
        ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
        synchronized (this) {
            TimeUnit u = TimeUnit.SECONDS;
            fut = stpe.scheduleWithFixedDelay(runner, 30, 30, u);
        }
    }
}
