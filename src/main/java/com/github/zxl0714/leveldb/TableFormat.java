package com.github.zxl0714.leveldb;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xiaolu on 4/30/15.
 */
public class TableFormat {

    public static final long TABLE_MAGIC_NUMBER = 0xdb4775248b80fb57L;
    public static final int BLOCK_TRAILER_SIZE = 5;

    public static class BlockHandle {

        public static final long MAX_ENCODED_LENGTH = 10 + 10;

        private long offset;
        private long size;

        public BlockHandle() { }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public int encodeTo(DataOutput out) throws IOException {
            int len = 0;
            len += Coding.putVarint64(out, offset);
            len += Coding.putVarint64(out, size);
            return len;
        }
    }

    public static class Footer {

        public static final long ENCODED_LENGTH = 2 * BlockHandle.MAX_ENCODED_LENGTH + 8;

        private BlockHandle metaindexHandle;
        private BlockHandle indexHandle;

        public Footer() { }

        public void setMetaindexHandle(BlockHandle h) {
            metaindexHandle = h;
        }

        public void setIndexHandle(BlockHandle h) {
            indexHandle = h;
        }

        public void encodeTo(DataOutput out) throws IOException {
            int len = 0;
            len += metaindexHandle.encodeTo(out);
            len += indexHandle.encodeTo(out);
            for (int i = len; i < BlockHandle.MAX_ENCODED_LENGTH * 2; i++) {
                out.writeByte(0);
            }
            Coding.putFixed32(out, (int) (TableFormat.TABLE_MAGIC_NUMBER));
            Coding.putFixed32(out, (int) (TableFormat.TABLE_MAGIC_NUMBER >>> 32));
        }
    }
}
