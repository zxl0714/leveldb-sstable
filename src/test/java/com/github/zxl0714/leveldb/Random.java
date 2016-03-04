package com.github.zxl0714.leveldb;

/**
 * Created by xiaolu on 4/30/15.
 */
public class Random {

    private long seed;

    public Random(long s) {
        seed = s & 0x7fffffff;
        if (seed == 0 || seed == 2147483647L) {
            seed = 1;
        }
    }

    public long next() {
        long M = 2147483647L;
        long A = 16807;

        seed = seed * A % M;
        return seed;
    }

    public int uniform(int n) {
        return (int) (next() % n);
    }

    public int skewed(int maxLog) {
        return uniform(1 << uniform(maxLog + 1));
    }
}
