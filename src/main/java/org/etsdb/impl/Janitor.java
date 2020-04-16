package org.etsdb.impl;

import org.dsa.iot.dslink.util.handler.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


class Janitor implements Runnable {

    static final Logger logger = LoggerFactory.getLogger(Janitor.class.getName());

    private Handler<Integer> handler;
    int lastFlushMillis;
    private final DatabaseImpl<?> db;
    private Thread thread;
    private FileLock fileLock;
    private int fileLockCheckInterval;
    private int flushInterval;
    private long nextFileLockCheck;
    private long nextFlush;

    /**
     * The number of meta closures that have been done since the last GC.
     */
    private int fileClosures;

    private volatile boolean running;

    Janitor(DatabaseImpl<?> db) {
        this.db = db;
    }

    void setFlushTimeHandler(Handler<Integer> handler) {
        this.handler = handler;
    }

    void setFileLockCheckInterval(int fileLockCheckInterval) {
        this.fileLockCheckInterval = fileLockCheckInterval;
    }

    void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    void lock() {
        fileLock = new FileLock(db, fileLockCheckInterval);
        fileLock.lock();
    }

    void initiate() {
        long now = System.currentTimeMillis();
        nextFileLockCheck = now + fileLockCheckInterval;
        nextFlush = now + flushInterval;

        running = true;

        thread = new Thread(this, "Maintenance " + db.getBaseDir().getPath());
        //thread.setDaemon(true);
        thread.setPriority(Thread.MAX_PRIORITY - 1);
        thread.start();
    }

    void terminate() {
        running = false;
    }

    @Override
    public void run() {
        while (running) {
            try {
                runImpl();
            } catch (Exception e) {
                logger.error("Error during Janitor run", e);
            }
        }

        fileLock.unlock();
    }

    private void runImpl() {
        long next = nextFileLockCheck;
        if (next > nextFlush) {
            next = nextFlush;
        }

        long sleep = next - System.currentTimeMillis();
        if (sleep > 0) {
            synchronized (this) {
                if (running) {
                    try {
                        wait(sleep);
                    } catch (InterruptedException e) {
                        // Ignore
                    }
                }
            }
        }

        if (!running) {
            return;
        }

        long now = System.currentTimeMillis();
        if (now >= nextFileLockCheck) {
            fileLock.update();
            nextFileLockCheck = now + fileLockCheckInterval;
        }

        if (now >= nextFlush) {
            long time = System.currentTimeMillis();
            boolean gc = false;
            try {
                fileClosures += db.flush(false);

                // A GC is required for the mapped buffers to be closed.
                if (running) {
                    if (db.tooManyFiles() || fileClosures > db.maxOpenFiles / 2) {
                        if (logger.isDebugEnabled()) {
                            logger.debug(
                                    "Running garbage collection. Files to close: " + fileClosures);
                        }
                        System.gc();
                        gc = true;
                        db.openFiles.addAndGet(-fileClosures);
                        fileClosures = 0;
                    }
                }
            } catch (IOException e) {
                logger.error("Exception during scheduled flush", e);
            }

            time = System.currentTimeMillis() - time;
            lastFlushMillis = (int) time;
            Handler<Integer> handler = this.handler;
            if (handler != null) {
                handler.handle(lastFlushMillis);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Write queue flush took " + time + " ms");
                logger.debug("write/s=" + db.getWritesPerSecond() + ", backdateCount=" +
                        db.getBackdateCount()
                        + ", writeCount=" + db.getWriteCount() + ", openFiles=" +
                        db.getOpenFiles() + ", forcedClose="
                        + db.getForcedClose());
            }

            // If the time that it took to do the last flush, times 10, is greater than the flush interval, use
            // the time * 10 as the interval. This prevents the flush from running too often. But, don't let the 
            // sleep time exceed the flush interval * 4.
            time *= 10;

            if (gc || time < flushInterval) {
                time = flushInterval;
            } else if (time > flushInterval * 4) {
                time = flushInterval * 4;
            }
            nextFlush = System.currentTimeMillis() + time;
        }
    }

    void join() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}
