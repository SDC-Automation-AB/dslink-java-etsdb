package org.etsdb.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;

/**
 * Modified from the H2 FileLock implementation.
 *
 * @author Matthew
 */
public class FileLock {

    private static final Logger logger = LoggerFactory.getLogger(FileLock.class.getName());

    private static final int SLEEP_GAP = 25;
    private static final int TIME_GRANULARITY = 2000;
    private final int sleep;
    private File file;
    private Path filePath;
    /**
     * The last time the lock file was written.
     */
    private long lastWrite;

    private boolean locked;
    private byte[] uniqueId;

    /**
     * Create a new file locking object.
     *
     * @param db    Database implementation to use
     * @param sleep the number of milliseconds to sleep
     */
    public FileLock(DatabaseImpl<?> db, int sleep) {
        this.file = new File(db.getBaseDir(), ".lock.db");
        this.filePath = Paths.get(file.getPath());
        this.sleep = sleep;
    }

    private static void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    /**
     * Lock the file if possible. A file may only be locked once.
     *
     * @throws RuntimeException if locking was not successful
     */
    public synchronized void lock() {
        if (locked) {
            throw new RuntimeException("already locked");
        }
        try {
            lockFile();
        } catch (IOException e) {
            throw new RuntimeException("Error locking file");
        }
        locked = true;
    }

    /**
     * Unlock the file. The watchdog thread is stopped. This method does nothing if the file is
     * already unlocked.
     */
    public synchronized void unlock() {
        if (!locked) {
            return;
        }

        locked = false;
        try {
            if (file != null) {
                if (!Arrays.equals(load(), uniqueId)) {
                    if (!file.delete()) {
                        logger.error("Failed to delete file: {}", file.getPath());
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("FileLock.unlock", e);
        } finally {
            file = null;
            filePath = null;
        }
    }

    /**
     * Save the lock file.
     */
    public synchronized void save() {
        try {
            Files.write(filePath, uniqueId);
            lastWrite = file.lastModified();
        } catch (Exception e) {
            throw new RuntimeException("Could not save properties " + file, e);
        }
    }

    /**
     * Load the uuid
     *
     * @return the properties
     */
    public byte[] load() {
        try {
            synchronized (this) {
                return Files.readAllBytes(filePath);
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not load lock file", e);
        }
    }

    private void waitUntilOld() {
        for (int i = 0; i < 2 * TIME_GRANULARITY / SLEEP_GAP; i++) {
            long last;
            synchronized (this) {
                last = file.lastModified();
            }
            long dist = System.currentTimeMillis() - last;
            if (dist < -TIME_GRANULARITY) {
                // lock file modified in the future -
                // wait for a bit longer than usual
                sleep(2 * sleep);
                return;
            } else if (dist > TIME_GRANULARITY) {
                return;
            }
            sleep(SLEEP_GAP);
        }

        throw new RuntimeException("Lock file recently modified");
    }

    private synchronized void setUniqueId() {
        uniqueId = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    }

    private synchronized void lockFile() throws IOException {
        setUniqueId();
        if (!file.createNewFile()) {
            waitUntilOld();
            save();
            sleep(2 * sleep);
            if (!Arrays.equals(load(), uniqueId)) {
                throw new RuntimeException("Locked by another process");
            }

            if (!file.delete()) {
                logger.error("Failed to delete file: {}", file.getPath());
            }
            if (!file.createNewFile()) {
                throw new RuntimeException("Another process was faster");
            }
        }
        save();
        sleep(SLEEP_GAP);
        if (!Arrays.equals(load(), uniqueId)) {
            file = null;
            filePath = null;
            throw new RuntimeException("Concurrent update");
        }
    }

    public synchronized void update() {
        if (file != null) {
            // trace.debug("watchdog check");
            try {
                if (!file.exists() || file.lastModified() != lastWrite) {
                    save();
                }
            } catch (OutOfMemoryError e) {
                // ignore
            } catch (Exception e) {
                logger.warn("FileLock.run", e);
            }
        }
    }
}
