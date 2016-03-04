package com.github.zxl0714.leveldb;

import java.util.Arrays;

/**
 * Created by xiaolu on 4/28/15.
 */
public class BytewiseComparator implements Comparator {

    public BytewiseComparator() {}

    @Override
    public int compare(Slice a, Slice b) {
        return a.compareTo(b);
    }

    @Override
    public String name() {
        return "leveldb.BytewiseComparator";
    }

    @Override
    public Slice findShortestSeparator(Slice start, Slice limit) {
        int minLength = Math.min(start.size(), limit.size());
        int diffIndex = 0;
        while ((diffIndex < minLength) && ((start.data()[diffIndex] == limit.data()[diffIndex]))) {
            diffIndex++;
        }
        if (diffIndex >= minLength) {
            return start;
        } else {
            byte diffByte = start.data()[diffIndex];
            if (Coding.byte2int(diffByte) < 0xff && Coding.byte2int(diffByte) + 1 < Coding.byte2int(limit.data()[diffIndex])) {
                byte[] t = Arrays.copyOf(start.data(), diffIndex + 1);
                t[diffIndex] = (byte) (Coding.byte2int(t[diffIndex]) + 1);
                return new Slice(t);
            } else {
                return start;
            }
        }
    }

    @Override
    public Slice findShortSuccessor(Slice key) {
        for (int i = 0; i < key.size(); i++) {
            if (key.data()[i] != (byte) 0xff) {
                byte[] t = Arrays.copyOf(key.data(), i + 1);
                t[i] = (byte) (Coding.byte2int(t[i]) + 1);
                return new Slice(t);
            }
        }
        return key;
    }
}
