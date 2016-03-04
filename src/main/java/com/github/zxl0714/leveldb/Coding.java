package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xiaolu on 4/28/15.
 */
public class Coding {

    public static byte[] encodeFixed32(int value) {
        return new byte[]{
                (byte) value,
                (byte) (value >>> 8),
                (byte) (value >>> 16),
                (byte) (value >>> 24)
        };
    }

    public static void putFixed32(DataOutput out, int value) throws IOException {
        out.write(encodeFixed32(value));
    }

    public static int putVarint32(DataOutput out, int value) throws IOException {
        int B = 128;
        if (value < 0) {
            out.writeByte(value | B);
            out.writeByte((value >>> 7) | B);
            out.writeByte((value >>> 14) | B);
            out.writeByte((value >>> 21) | B);
            out.writeByte(value >>> 28);
            return 5;
        } else if (value < (1 << 7)) {
            out.writeByte(value);
            return 1;
        } else if (value < (1 << 14)) {
            out.writeByte(value | B);
            out.writeByte(value >>> 7);
            return 2;
        } else if (value < (1 << 21)) {
            out.writeByte(value | B);
            out.writeByte((value >>> 7) | B);
            out.writeByte(value >>> 14);
            return 3;
        } else if (value < (1 << 28)) {
            out.writeByte(value | B);
            out.writeByte((value >>> 7) | B);
            out.writeByte((value >>> 14) | B);
            out.writeByte(value >>> 21);
            return 4;
        }
        throw new IOException("something wrong");
    }

    public static byte[] encodeFixed64(long value) {
        return new byte[]{
                (byte) value,
                (byte) (value >>> 8),
                (byte) (value >>> 16),
                (byte) (value >>> 24),
                (byte) (value >>> 32),
                (byte) (value >>> 40),
                (byte) (value >>> 48),
                (byte) (value >>> 56)
        };
    }

    public static void putFixed64(DataOutput out, long value) throws IOException {
        out.write(encodeFixed64(value));
    }

    public static long decodeFixed64(byte[] data, int offset) {
        Preconditions.checkArgument(data.length >= offset + 8);

        return  (byte2long(data[offset + 7]) << 56) | (byte2long(data[offset + 6]) << 48)
                | (byte2long(data[offset + 5]) << 40) | (byte2long(data[offset + 4]) << 32)
                | (byte2long(data[offset + 3]) << 24) | (byte2long(data[offset + 2]) << 16)
                | (byte2long(data[offset + 1]) << 8) | byte2long(data[offset]);
    }

    public static int putVarint64(DataOutput out, long value) throws IOException {
        int B = 128;
        int length = 0;
        while ((value >>> 7) != 0) {
            out.writeByte((int) ((value & (B - 1)) | B));
            value >>>= 7;
            length++;
        }
        out.writeByte((int) value);
        return length + 1;
    }

    public static int byte2int(byte b) {
        return b & 0xff;
    }

    public static long byte2long(byte b) {
        return b & 0xffL;
    }

    public static int decodeFixed32(byte[] data, int offset) {
        Preconditions.checkArgument(data.length >= offset + 4);

        return (byte2int(data[offset + 3]) << 24) | (byte2int(data[offset + 2]) << 16)
                | (byte2int(data[offset + 1]) << 8) | byte2int(data[offset]);
    }

    public static void putLengthPrefixedSlice(DataOutput dst, Slice value) throws IOException {
        putVarint32(dst, value.size());
        dst.write(value.data(), 0, value.size());
    }
}
