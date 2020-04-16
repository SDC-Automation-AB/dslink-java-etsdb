package org.etsdb.impl;

import org.dsa.iot.shared.SharedObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class CorruptionScanner {

    private static final List<File> emptyList = Collections.emptyList();
    private static final Logger logger = LoggerFactory.getLogger(CorruptionScanner.class.getName());

    private final DatabaseImpl<?> db;
    private final AtomicInteger threads = new AtomicInteger(0);

    CorruptionScanner(DatabaseImpl<?> db) {
        this.db = db;
    }

    public static void main(String[] args) throws IOException {
        CorruptionScanner scanner = new CorruptionScanner(null);
        File data = new File("corruption/1175.data");
        long time = System.currentTimeMillis();
        scanner.checkFile(data);
        System.out.println("Time: " + (System.currentTimeMillis() - time));

        for (int i = 101; i < 117; i++) {
            data = new File("corruption/" + i + ".data");
            scanner.checkFile(data);
        }
    }

    void scan() {
        scan(db.getBaseDir());
    }

    private void scan(File parent) {
        File[] subdirs = parent.listFiles();
        if (subdirs != null) {
            for (File subdir : subdirs) {
                File[] seriesDirs = subdir.listFiles();
                if (seriesDirs != null) {
                    for (File seriesDir : seriesDirs) {
                        checkSeriesDir(seriesDir);
                        deepScan(seriesDir);
                    }
                }
            }
        }
        synchronized (this) {
            while (threads.get() > 0) {
                try {
                    wait(1000);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    private void deepScan(File parent) {
        if (!parent.isDirectory()) {
            return;
        }
        File[] subdirs = parent.listFiles();
        if (subdirs != null) {
            for (File subdir : subdirs) {
                checkSeriesDir(subdir);
                deepScan(subdir);
            }
        }
    }

    private void checkSeriesDir(final File seriesDir) {
        if (!seriesDir.isDirectory()) {
            return;
        }
        File[] files = seriesDir.listFiles();
        if ((files == null) || (files.length == 0)) {
            return;
        }
        final List<File> temps = getFiles(files, ".temp");
        final List<File> datas = getFiles(files, ".data");
        final List<File> metas = getFiles(files, ".meta");
        if (temps.isEmpty() && datas.isEmpty() && metas.isEmpty()) {
            return;
        }
        threads.incrementAndGet();
        SharedObjects.getDaemonThreadPool().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    checkSeriesDir(seriesDir, temps, datas, metas);
                } catch (Exception x) {
                    logger.error(seriesDir.getPath(), x);
                } finally {
                    threads.decrementAndGet();
                    synchronized (CorruptionScanner.this) {
                        CorruptionScanner.this.notify();
                    }
                }
            }
        });
    }

    private void checkSeriesDir(File seriesDir,
            List<File> temps,
            List<File> datas,
            List<File> metas) throws IOException {
        // temp files.
        if (!temps.isEmpty()) {
            for (File temp : temps) {
                long shardId = Utils.getShardId(temp.getName(), 10);
                File data = new File(seriesDir, shardId + ".data");
                File meta = new File(seriesDir, shardId + ".meta");

                if (data.exists()) {
                    // If the data file exists, then just delete the file
                    logger.warn("Found temp file " + temp + " with existing data file. Deleting.");
                    Utils.deleteWithRetry(temp);
                } else if (meta.exists()) {
                    // If the meta file exists, then rename the temp file to data, and delete the meta file so that it gets
                    // recreated.
                    logger.warn(
                            "Found temp file " + temp +
                                    " without data but with meta file. Moving.");
                    Utils.renameWithRetry(temp, data);
                    Utils.deleteWithRetry(meta);
                } else {
                    // Otherwise, just delete the temp file.
                    logger.warn("Found temp file " + temp +
                            " without data, meta file, or content. Deleting.");
                    Utils.deleteWithRetry(temp);
                }
            }
        }

        if (!datas.isEmpty()) {
            for (File data : datas) {
                long shardId = Utils.getShardId(data.getName());
                boolean found = false;
                for (int i = metas.size() - 1; i >= 0; i--) {
                    File meta = metas.get(i);
                    if (Utils.getShardId(meta.getName()) == shardId) {
                        metas.remove(i);
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    logger.warn("Data file without meta file in series " + seriesDir.getName() +
                            ", shard " + shardId + ".");
                }
            }
        }

        // If there are any files left in the meta list, then they should just be deleted.
        if (!metas.isEmpty()) {
            for (File meta : metas) {
                logger.warn("Meta file without data file at " + meta + ". Deleting file");
                Utils.deleteWithRetry(meta);
            }
        }

        if (!datas.isEmpty()) {
            for (File data : datas) {
                checkFile(data);
            }
        }
    }

    private List<File> getFiles(File[] files, String suffix) {
        List<File> result = null;
        for (File file : files) {
            if (!file.isDirectory() && file.getName().endsWith(suffix)) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                result.add(file);
            }
        }
        if (result == null) {
            return emptyList;
        }
        return result;
    }

    private void checkFile(File data) throws IOException {
        long position = 0;
        // Start a detect/fix loop.
        while (true) {
            position = findCorruption(data, position);
            if (position == -1) {
                break;
            }

            // If any corruption was found, delete the meta file so that it gets recreated.
            Utils.deleteWithRetry(
                    new File(data.getParent(), Utils.getShardId(data.getName()) + ".meta"));

            logger.warn("Corruption detected in " + data + " at position " + position);
            fixCorruption(data, position);
        }
    }

    private long findCorruption(File data, long startPosition) throws IOException {
        ChecksumInputStream in = null;
        try {
            ScanInfo scanInfo = new ScanInfo();
            in = new ChecksumInputStream(data);

            if (startPosition > 0) {
                Utils.skip(in, startPosition);
            }

            while (true) {
                long position = in.position();
                if (!checkRow(in, scanInfo)) {
                    return position;
                }
                if (scanInfo.isEof()) {
                    break;
                }
            }
        } finally {
            Utils.closeQuietly(in);
        }

        return -1;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void fixCorruption(File data, long badRowposition) throws IOException {
        ChecksumInputStream in = null;
        try {
            ScanInfo scanInfo = new ScanInfo();
            in = new ChecksumInputStream(data);
            Utils.skip(in, badRowposition);

            while (true) {
                in.read();
                in.mark(Utils.MAX_DATA_LENGTH + 10);
                long position = in.position();
                if (!checkRow(in, scanInfo)) {
                    // Bad row. Keep looking for a good one.
                    in.reset();
                    continue;
                } else if (scanInfo.isEof()) {
                    // We reached the EOF before finding a good row. Splice off the end of the file.
                    Utils.closeQuietly(in);
                    cut(data, badRowposition, position);
                    break;
                }

                // Now, look for another 2 to add confidence that it's for real.
                if (!checkRow(in, scanInfo)) {
                    // Bad row. The previous "good" one was probably just a fluke. Keep looking.
                    in.reset();
                    continue;
                } else if (scanInfo.isEof()) {
                    // We reached the EOF before finding a second good row. We'll assume the row is good and cut out
                    // the bad bit.
                    Utils.closeQuietly(in);
                    cut(data, badRowposition, position);
                    break;
                }

                // Found 2 good ones (or we're at EOF). Looking good...
                if (!checkRow(in, scanInfo)) {
                    // Oy. It's a stretch for there to be two false positives in a row, but we're going to call it a 
                    // fluke and keep looking.
                    in.reset();
                    continue;
                }

                // Ok, good enough, or we're at the EOF. Either way, cut out the bad row and call it fixed.
                Utils.closeQuietly(in);
                cut(data, badRowposition, position);
                break;
            }
        } finally {
            Utils.closeQuietly(in);
        }
    }

    private boolean checkRow(ChecksumInputStream in, ScanInfo scanInfo) throws IOException {
        try {
            DataShard._readSample(in, scanInfo);
        } catch (BadRowException e) {
            return false;
        }

        if (scanInfo.isEof()) // No row was read because we normally reached the EOF.
        {
            return true;
        }

        // ??? Check that the record's ts is greater than 0, greater than the last, and less than the shard max.
        // Verify the checksum.
        return in.checkSum();

    }

    /**
     * Deletes bytes in a file
     * <p>
     *
     * @param data the data file to splice
     * @param from the inclusive position to delete from
     * @param to   the exclusive position to delete to
     * @throws IOException
     */
    private void cut(File data, long from, long to) throws IOException {
        File temp = new File(data.getParentFile(), "temp");
        FileInputStream in = null;
        FileOutputStream out = null;

        logger.warn("Cutting corrupt data in " + data + " at " + from + ", length " + (to - from));

        try {
            in = new FileInputStream(data);
            out = new FileOutputStream(temp);
            byte[] buf = new byte[8192];

            // Write to the from position.
            copy(in, out, from, buf);

            // Skip the difference in the input stream.
            Utils.skip(in, to - from);

            // Write the remainder of the input stream.
            copy(in, out, data.length() - to, buf);
        } finally {
            Utils.closeQuietly(in);
            Utils.closeQuietly(out);
        }

        Utils.deleteWithRetry(data);
        Utils.renameWithRetry(temp, data);
    }

    private void copy(InputStream in, OutputStream out, long length, byte[] buf)
            throws IOException {
        while (length > 0) {
            int chunk = buf.length;
            if (length < buf.length) {
                chunk = (int) length;
            }
            int read = in.read(buf, 0, chunk);
            if (read == -1) {
                break;
            }
            out.write(buf, 0, read);
            length -= read;
        }
    }

}
