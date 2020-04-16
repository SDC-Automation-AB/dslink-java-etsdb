package org.etsdb.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

class ChecksumInputStream extends InputStream implements ChecksumInput {

    private final InputStream delegate;

    private byte sum;
    private long position;
    private boolean eof;

    private long markPosition = -1;
    private byte markSum;

    ChecksumInputStream(File dataFile) {
        if (dataFile.exists()) {
            try {
                this.delegate = new BufferedInputStream(new FileInputStream(dataFile), 32768);
            } catch (FileNotFoundException e) {
                // This should not happen because we just checked that the file exists.
                throw new RuntimeException(e);
            }
        } else {
            delegate = null;
            eof = true;
        }
    }

    @Override
    public boolean checkSum() throws IOException {
        if (eof) {
            return false;
        }

        byte b = (byte) delegate.read();
        position++;
        boolean match = b == sum;
        sum = 0;
        return match;
    }

    @Override
    public int read() throws IOException {
        if (eof) {
            return -1;
        }

        int i = delegate.read();
        if (i == -1) {
            eof = true;
        } else {
            position++;
            sum += (byte) i;
        }

        return i;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (eof) {
            return -1;
        }

        int count = delegate.read(b, off, len);
        if (count == -1) {
            eof = true;
        } else {
            position += count;
            for (int i = 0; i < count; i++) {
                sum += b[i + off];
            }
        }
        return count;
    }

    long position() {
        return position;
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public boolean isEof() {
        return eof;
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegate.mark(readlimit);
        markPosition = position;
        markSum = sum;
        sum = 0;
    }

    @Override
    public synchronized void reset() throws IOException {
        delegate.reset();
        if (markPosition != -1) {
            position = markPosition;
            sum = markSum;
            markPosition = -1;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        long l = delegate.skip(n);
        position += l;
        return l;
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
        }
    }
}
