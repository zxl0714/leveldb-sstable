package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by xiaolu on 5/5/15.
 */
public class LogWriter implements Closeable, Flushable {

    public static class RecordType {
        public static final int ZERO_TYPE = 0;
        public static final int FULL_TYPE = 1;
        public static final int FIRST_TYPE = 2;
        public static final int MIDDLE_TYPE = 3;
        public static final int LAST_TYPE = 4;
    }

    public static final int MAX_RECORD_TYPE = RecordType.LAST_TYPE;
    public static final int BLOCK_SIZE = 32768;
    public static final int HEADER_SIZE = 4 + 2 + 1;

    private final OutputStream out;
    private final int[] typeCrc;

    private int blockOffset = 0;

    public LogWriter(OutputStream out) {
        this.out = out;
        this.typeCrc = new int[MAX_RECORD_TYPE + 1];
        for (int i = 0; i <= MAX_RECORD_TYPE; i++) {
            typeCrc[i] = Crc32c.value(new byte[]{(byte) i}, 1);
        }
    }

    public void addRecord(Slice slice) throws IOException {
        int left = slice.size();
        byte[] data = slice.data();
        boolean begin = true;
        do {
            int leftover = BLOCK_SIZE - blockOffset;
            Preconditions.checkState(leftover >= 0);
            if (leftover < HEADER_SIZE) {
                Preconditions.checkState(HEADER_SIZE == 7);
                for (int i = 0; i < leftover; i++) {
                    out.write(0);
                }
                blockOffset = 0;
            }
            Preconditions.checkState(BLOCK_SIZE - blockOffset - HEADER_SIZE >= 0);
            int avail = BLOCK_SIZE - blockOffset - HEADER_SIZE;
            int fragmentLength = (left < avail) ? left : avail;
            int type;
            boolean end = (left == fragmentLength);
            if (begin && end) {
                type = RecordType.FULL_TYPE;
            } else if (begin) {
                type = RecordType.FIRST_TYPE;
            } else if (end) {
                type = RecordType.LAST_TYPE;
            } else {
                type = RecordType.MIDDLE_TYPE;
            }
            emitPhysicalRecord(type, data, slice.size() - left, fragmentLength);
            left -= fragmentLength;
            begin = false;
        } while (left > 0);
    }

    private void emitPhysicalRecord(int type, byte[] data, int offset, int size) throws IOException {
        Preconditions.checkArgument(size <= 0xffff);
        Preconditions.checkArgument(blockOffset + HEADER_SIZE + size <= BLOCK_SIZE);

        byte[] buf = new byte[HEADER_SIZE];
        buf[4] = (byte) (size & 0xff);
        buf[5] = (byte) (size >>> 8);
        buf[6] = (byte) type;

        int crc = Crc32c.extend(typeCrc[type], data, offset, size);
        crc = Crc32c.mask(crc);
        System.arraycopy(Coding.encodeFixed32(crc), 0, buf, 0, 4);

        out.write(buf);
        out.write(data, offset, size);
        out.flush();

        blockOffset += HEADER_SIZE + size;
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }
}
