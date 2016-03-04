package com.github.zxl0714.leveldb;

/**
 * Created by xiaolu on 4/28/15.
 */
public interface Comparator {
    int compare(Slice a, Slice b);
    String name();
    Slice findShortestSeparator(Slice start, Slice limit);
    Slice findShortSuccessor(Slice key);
}
