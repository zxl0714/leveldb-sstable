package com.github.zxl0714.leveldb;

/**
 * Created by Xiaolu on 2015/4/28.
 */
public class Hash {

    public static int hash(byte[] data, int n, int seed32) {
        // Similar to murmur hash
        long seed = seed32 & 0xffffffffL;
        long m = 0xc6a4a793L;
        int r = 24;
        long h = (seed ^ (n * m)) & 0xffffffffL;

        int i = 0;
        for (; i + 4 <= n; i += 4) {
            long w = Coding.decodeFixed32(data, i) & 0xffffffffL;
            h += w;
            h *= m;
            h &= 0xffffffffL;
            h ^= h >>> 16;
        }

        switch (n - i) {
            case 3:
                h += Coding.byte2int(data[i + 2]) << 16;
            case 2:
                h += Coding.byte2int(data[i + 1]) << 8;
            case 1:
                h += Coding.byte2int(data[i]);
                h *= m;
                h &= 0xffffffffL;
                h ^= h >>> r;
                break;
        }
        return (int) (h & 0xffffffffL);
    }
}
