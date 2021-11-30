package org.etsdb.impl;

import org.dsa.iot.dslink.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Since most databases will be on the same file store, this coalesces the call to get the available
 * file space.  Previously, it was using a lot of cpu when each database made its own call.
 */
class AvailableSpace {

    private static final long INTERVAL_MILLIS = 10000;
    private static final Logger LOGGER = LoggerFactory.getLogger(AvailableSpace.class);
    private static final Map<String, Info> map = new ConcurrentHashMap<>();
    private static final ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
    private static volatile ScheduledFuture<?> future;
    private static volatile int interest = 0;

    public static long getAvailableSpace(File dir) {
        try {
            Path path = Paths.get(dir.getPath());
            FileStore fs = Files.getFileStore(path);
            String name = name(fs);
            Info info = map.get(name);
            if (info == null) {
                return -1;
            }
            return info.availableSpace;
        } catch (Exception x) {
            LOGGER.debug(dir.getParent(), x);
        }
        return -1;
    }

    public static synchronized void registerInterest(File dir) {
        try {
            Path path = Paths.get(dir.getPath());
            FileStore fs = Files.getFileStore(path);
            String name = name(fs);
            Info info = map.get(name);
            if (info == null) {
                info = new Info();
                info.fileStore = fs;
                map.put(name, info);
            } else {
                return;
            }
            info.interest++;
            if (++interest == 1) {
                long delay = (long) (INTERVAL_MILLIS * Math.random());
                future = stpe.schedule(AvailableSpace::update, delay, TimeUnit.MILLISECONDS);
            }
        } catch (Exception x) {
            LOGGER.error(dir.getParent(), x);
        }
    }

    public static synchronized void unregisterInterest(File dir) {
        try {
            Path path = Paths.get(dir.getPath());
            FileStore fs = Files.getFileStore(path);
            String name = name(fs);
            Info info = map.get(name);
            if (info == null) {
                return;
            }
            if (--info.interest <= 0) {
                map.remove(name);
            }
            if (--interest <= 0) {
                ScheduledFuture<?> tmp = future;
                future = null;
                if (tmp != null) {
                    tmp.cancel(false);
                }
            }
        } catch (Exception x) {
            LOGGER.error(dir.getParent(), x);
        }
    }

    private static String name(FileStore store) {
        String name = store.name();
        if (name == null) {
            name = store.toString();
        }
        return name;
    }

    private static void update() {
        future = null;
        long time = System.currentTimeMillis();
        for (Info info : map.values()) {
            if (interest <= 0) {
                return;
            }
            try {
                info.availableSpace = info.fileStore.getUsableSpace();
            } catch (Exception x) {
                info.availableSpace = -1;
                LOGGER.debug(info.fileStore.toString(), x);
            }
        }
        if (interest > 0) {
            time = System.currentTimeMillis() - time;
            time = Math.max(INTERVAL_MILLIS, time * 2);
            future = stpe.schedule(AvailableSpace::update, time, TimeUnit.MILLISECONDS);
        }
    }

    private static class Info {

        long availableSpace;
        int interest;
        FileStore fileStore;
    }
}