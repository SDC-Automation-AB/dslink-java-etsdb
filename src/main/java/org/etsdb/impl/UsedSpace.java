package org.etsdb.impl;

import org.dsa.iot.dslink.util.Objects;
import org.etsdb.util.DirectoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Updates disk usage for all databases on a single thread.  Was consuming too much CPU when each
 * database did it independently.
 */
class UsedSpace {

    private static final long INTERVAL_MILLIS = 10000;
    private static final Logger LOGGER = LoggerFactory.getLogger(UsedSpace.class);
    private static final Map<String, Info> map = new ConcurrentHashMap<>();
    private static final ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
    private static volatile ScheduledFuture<?> future;
    private static volatile int interest = 0;

    public static long getUsedSpace(File dir) {
        try {
            Info info = map.get(dir.getPath());
            if (info == null) {
                return -1;
            }
            return info.usedSpace;
        } catch (Exception x) {
            LOGGER.debug(dir.getParent(), x);
        }
        return -1;
    }

    public static synchronized void registerInterest(File dir) {
        try {
            String path = dir.getPath();
            Info info = map.get(path);
            if (info == null) {
                info = new Info();
                info.path = Paths.get(path);
                map.put(path, info);
            } else {
                return;
            }
            if (++interest == 1) {
                long delay = (long) (INTERVAL_MILLIS * Math.random());
                future = stpe.schedule(UsedSpace::update, delay, TimeUnit.MILLISECONDS);
            }
        } catch (Exception x) {
            LOGGER.error(dir.getParent(), x);
        }
    }

    public static synchronized void unregisterInterest(File dir) {
        try {
            map.remove(dir.getPath());
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

    private static void update() {
        future = null;
        long time = System.currentTimeMillis();
        for (Info info : map.values()) {
            if (interest <= 0) {
                return;
            }
            try {
                info.usedSpace = DirectoryUtils.getSize(info.path).getSize();
            } catch (Exception x) {
                info.usedSpace = -1;
                LOGGER.debug(info.path.toString(), x);
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException ignore) {
            }
        }
        if (interest > 0) {
            time = System.currentTimeMillis() - time;
            time = Math.max(INTERVAL_MILLIS, time * 2);
            future = stpe.schedule(UsedSpace::update, time, TimeUnit.MILLISECONDS);
        }
    }

    private static class Info {

        Path path;
        long usedSpace;
    }
}