package com.github.zxl0714.leveldb;

/**
 * Created by xiaolu on 4/28/15.
 */
public class Options {
    public int blockRestartInterval = 4;
    public Comparator comparator = new BytewiseComparator();
    public FilterPolicy filterPolicy = null;
    public int blockSize = 4096;
    public int compression = CompressionType.SNAPPY_COMPRESSION;

    public static class CompressionType {
        public final static int NO_COMPRESSION = 0x0;
        public final static int SNAPPY_COMPRESSION = 0x1;
    }

    public Options() { }

    public Options(Options options) {
        this.blockRestartInterval = options.blockRestartInterval;
        this.comparator = options.comparator;
        this.filterPolicy = options.filterPolicy;
        this.blockSize = options.blockSize;
        this.compression = options.compression;
    }
}
