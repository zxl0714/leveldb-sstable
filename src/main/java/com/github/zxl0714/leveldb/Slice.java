package com.github.zxl0714.leveldb;

import com.google.common.annotations.VisibleForTesting;

/**
 * Created by xiaolu on 4/28/15.
 */
public class Slice implements Comparable<Slice> {

    private final byte[] data;
    private final int size;

    public Slice() {
        data = new byte[0];
        size = 0;
    }

    public Slice(byte[] data) {
        this.data = data;
        this.size = data.length;
    }

    public Slice(byte[] data, int size) {
        this.data = data;
        this.size = size;
    }

    @VisibleForTesting
    Slice(String data) {
        this.data = data.getBytes();
        this.size = this.data.length;
    }

    public int size() {
        return size;
    }

    public byte[] data() {
        return data;
    }

    @Override
    public int compareTo(Slice b) {
        int minLen = Math.min(size(), b.size());
        for (int i = 0; i < minLen; i++) {
            if (Coding.byte2int(data[i]) < Coding.byte2int(b.data()[i])) {
                return -1;
            }
            if (Coding.byte2int(data[i]) > Coding.byte2int(b.data()[i])) {
                return 1;
            }
        }
        if (size < b.size()) {
            return -1;
        }
        if (size > b.size()) {
            return 1;
        }
        return 0;
    }
}
