package com.github.zxl0714.leveldb;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.github.zxl0714.leveldb.Hash.hash;

/**
 * Created by Xiaolu on 2015/4/28.
 */
public class BloomFilterPolicy implements FilterPolicy {

    private final int bitsPerKey;
    private final int k;

    public BloomFilterPolicy(int bitsPerKey) {
        this.bitsPerKey = bitsPerKey;
        int k = (int) (bitsPerKey * 0.69);
        if (k < 1) {
            k = 1;
        }
        if (k > 30) {
            k = 30;
        }
        this.k = k;
    }

    public static int bloomHash(Slice key) {
        return hash(key.data(), key.size(), 0xbc9f1d34);
    }

    @Override
    public String name() {
        return "leveldb.BuiltinBloomFilter2";
    }

    @Override
    public int createFilter(List<Slice> keys, DataOutput dst) throws IOException {
        int n = keys.size();
        int bits = n * bitsPerKey;
        if (bits < 64) {
            bits = 64;
        }
        int bytes = (bits + 7) / 8;
        bits = bytes * 8;
        byte[] filterBlockData = new byte[bytes];
        Arrays.fill(filterBlockData, (byte) 0);
        for (Slice key : keys) {
            long h = bloomHash(key) & 0xffffffffL;
            long delta = ((h >>> 17) | (h << 15)) & 0xffffffffL;
            for (int j = 0; j < k; j++) {
                int bitpos = (int) (h % bits);
                filterBlockData[bitpos / 8] |= (1 << (bitpos % 8));
                h = (h + delta) & 0xffffffffL;
            }
        }
        dst.write(filterBlockData);
        dst.writeByte(k);
        return filterBlockData.length + 1;
    }
}
