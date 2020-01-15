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

    public void addDb(Db db) {
    	synchronized(databases) {
	        if (!databases.contains(db)) {
	            databases.add(db);
	        }
    	}
    }

    public void removeDb(Db db) {
    	synchronized (databases) {
    		databases.remove(db);
    	}
    }

    public void stop() {
        running = false;
        synchronized (this) {
            if (fut != null) {
                fut.cancel(true);
            }
        }
    }

    public void setupPurger() {
        running = true;
        Runnable runner = new Runnable() {
            @Override
            public void run() {
            	synchronized (databases) {
                    for (Db db : databases) {
                        if (!(db.isPurgeable() && running)) {
                            continue;
                        }
    
                        File path = db.getPath();
                        long curr = path.getUsableSpace();
                        long request = db.getDiskSpaceRemaining();
                        long delCount = 0;
                        long shardDelCount = 0;
                        LOGGER.info("Deciding whether to purge");
                        LOGGER.info("curr = " + curr + " , request = " + request);
                        if (curr - request <= 0) {
                            if (!running) {
                                break;
                            }
                            LOGGER.info("Going to purge");
                            DatabaseImpl<ByteData> realDb = db.getDb();
                            List<String> series = db.getSanitizedSeriesIds();
//                            LOGGER.info("Purge Step 1");
                            while (curr - request <= 0) {
//                            	LOGGER.info("Purge Step 2");
    	                        TimeRange range = realDb.getTimeRange(series);
    	                        if (range == null || range.isUndefined()) {
    	                            break;
    	                        }
//    	                        LOGGER.info("Purge Step 3");
    	                        long from = range.getFrom();
    	                        long to = getToFromFrom(from);
    	                        for (String s : series) {
//    	                        	LOGGER.info("Purge Step 4");
    	                        	delCount += realDb.delete(s, from, to);
    	                        	int openShards = realDb.getOpenShards();
    	                        	realDb.purge(s, to);
    	                        	shardDelCount += (openShards - realDb.getOpenShards());
    	                        }
//    	                        LOGGER.info("Purge Step 5");
    	                        if (delCount <= 0 && shardDelCount <= 0) {
    	                            break;
    	                        }
//    	                        LOGGER.info("Purge Step 6");
    	                        curr = path.getUsableSpace();
    	                        LOGGER.info("Purge progress: deleted " + delCount + "records so far");
    	                        LOGGER.info("curr = " + curr + " , request = " + request);
                            }
                        }
                        if (delCount > 0) {
                            String p = path.getPath();
                            LOGGER.info("Deleted {} records from {}", delCount, p);
                        }
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
    
    private static long getToFromFrom(long from) {
        long diff = System.currentTimeMillis() - from;
        long range = (long) (diff * .15);
        if (range < 3600000) {
            range = 3600000;
        }
        return from + range;
    }
}
