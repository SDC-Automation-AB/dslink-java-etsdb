package org.etsdb.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {

    // This value must have at least one byte.
    static final byte[] SAMPLE_HEADER = {(byte) 0xfe, (byte) 0xed};

    public static final int MAX_DATA_LENGTH = 8192; // 8K
    private static final Logger logger = LoggerFactory.getLogger(Utils.class.getName());
    private static int SHARD_BITS = 30;
    // File IO retries
    private static final int FILE_IO_RETRIES = 30;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
    private static final Date date = new Date(0);

    public static void setShardBits(int bits) {
        SHARD_BITS = bits;
    }

    public static File getSeriesDir(File baseDir, String seriesId) {
        return new File(baseDir, getShardDirectory(seriesId) + File.separator + seriesId);
    }

    /**
     * The top level has maximum 100 directories.
     *
     * @param seriesId ID
     * @return the top level directory in which the series data is stored.
     */
    public static long getShardDirectory(String seriesId) {
        int i = seriesId.hashCode();
        if (i < 0) {
            i = -i;
        }
        return i % 100;
    }

    /**
     * Determines the name of the shard file for the given time stamp.
     *
     * @param ts the time stamp of the sample
     * @return the name of the shard file.
     */
    public static long getShardId(long ts) {
        return ts >> SHARD_BITS;
    }

    public static long getShardId(String filename) {
        return getShardId(filename, 5);
    }

    public static long getShardId(String filename, int suffixLength) {
        return Long.parseLong(filename.substring(0, filename.length() - suffixLength));
    }

    /**
     * Returns the offset of the given ts within the given shard.
     *
     * @param shardId ID of the shard
     * @param ts      Timestamp
     * @return offset
     */
    public static long getOffsetInShard(long shardId, long ts) {
        long tsShardId = getShardId(ts);
        if (tsShardId < shardId) {
            return 0;
        }
        if (tsShardId == shardId) {
            return getSampleOffset(ts);
        }
        return 0x40000000;
    }

    /**
     * Determines the offset of the sample within the shard file.
     *
     * @param ts the time stamp of the sample
     * @return the offset of the sample within the shard file.
     */
    public static long getSampleOffset(long ts) {
        return ts & 0x3fffffff;
    }

    public static long getTimestamp(long shardFile, long offset) {
        return (shardFile << SHARD_BITS) | offset;
    }

    public static void closeQuietly(Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (IOException e) {
            logger.warn("Exception during close", e);
        }
    }

    public static String prettyTimestamp(long ts) {
        synchronized (sdf) {
            date.setTime(ts);
            return sdf.format(date);
        }
    }

    public static void writeCompactInt(OutputStream out, int i) throws IOException {
        // Convert to unsigned.
        putCompactLong(out, i & 0xffffffffL);
    }

    public static void putCompactLong(OutputStream out, long l) throws IOException {
        if (l < 0) {
            throw new IllegalArgumentException("Cannot store negative numbers");
        }
        while (true) {
            if (l < 128) {
                out.write((byte) l);
                break;
            }
            out.write(((byte) l) | 0x80);
            l = l >> 7;
        }
    }

    public static int readCompactInt(Input in) throws IOException {
        return (int) readCompactLong(in);
    }

    public static long readCompactLong(Input in) throws IOException {
        long result = 0;
        int count = 0;
        while (true) {
            long l = in.read();
            if (l == -1) {
                throw new IOException("EOF");
            }
            result |= (l & 0x7f) << (count++ * 7);
            if (l < 128) {
                break;
            }
        }
        return result;
    }

    public static boolean skip(InputStream in, long n) throws IOException {
        while (n > 0) {
            long skipped = in.skip(n);
            if (skipped == 0) {
                // Check if we've reached the EOF
                in.mark(1);
                int i = in.read();
                in.reset();
                if (i == -1) {
                    return true;
                }
            }
            n -= skipped;
        }

        return false;
    }

    public static void deleteWithRetry(File file) throws IOException {
        if (!file.exists()) {
            return;
        }

        int retries = FILE_IO_RETRIES;
        while (true) {
            if (file.delete()) {
                break;
            }

            if (retries == 0) {
                throw new IOException("Failed to delete " + file);
            }

            retries--;
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to delete " + file + ", " + retries + " retries left");
            }
            sleep(FILE_IO_RETRIES - retries);
        }
    }

    public static void renameWithRetry(File from, File to) throws IOException {
        int retries = FILE_IO_RETRIES;
        while (true) {
            if (from.renameTo(to)) {
                break;
            }
            if (retries == 0) {
                throw new IOException("Failed to rename " + from + " to " + to);
            }

            retries--;
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to rename " + from + " to " + to + ", " + retries +
                        " retries left");
            }
            sleep(FILE_IO_RETRIES - retries);
        }
    }

    public static void delete(File file) throws IOException {
        if (!file.exists()) {
            return;
        }

        String message = null;
        while (true) {
            try {
                _delete(file);
                break;
            } catch (IOException e) {
                if (e.getMessage() != null) {
                    if (message == null) {
                        message = e.getMessage();
                    } else {
                        if (e.getMessage().equals(message)) {
                            throw e;
                        }
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    private static void _delete(File file) throws IOException {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File file1 : files) {
                    delete(file1);
                }
            }
        }

        if (!file.delete()) {
            throw new IOException("Failed to delete " + file);
        }
    }

    /**
     * Returns true if the given directory was deleted.
     */
    public static boolean deleteEmptyDirs(File file) {
        if (file.isDirectory()) {
            // Recurse to leaves.
            File[] files = file.listFiles();
            boolean empty = true;
            if (files != null && files.length > 0) {
                for (File subFile : files) {
                    if (!deleteEmptyDirs(subFile)) {
                        empty = false;
                    }
                }
            }
            if (empty) {
                if (!file.delete()) {
                    logger.error("Failed to delete empty dir: {}", file.getPath());
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    public static void write4ByteUnsigned(OutputStream out, long l) throws IOException {
        out.write((byte) (l >> 24));
        out.write((byte) (l >> 16));
        out.write((byte) (l >> 8));
        out.write((byte) l);
    }

    public static long read4ByteUnsigned(Input in) throws IOException {
        long l = 0;
        l |= (in.read() & 0xff) << 24;
        l |= (in.read() & 0xff) << 16;
        l |= (in.read() & 0xff) << 8;
        l |= in.read() & 0xff;
        return l;
    }

    public static void sleep(int time) {
        try {
            if (time > 0) {
                Thread.sleep(time);
            }
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    public static int compareLong(long l1, long l2) {
        if (l1 < l2) {
            return -1;
        }
        if (l1 == l2) {
            return 0;
        }
        return 1;
    }

    public static byte[] copy(byte[] data, int off, int len) {
        byte[] b = new byte[len];
        System.arraycopy(data, off, b, 0, len);
        return b;
    }
}
