package com.github.zxl0714.leveldb;

/**
 * Created by Xiaolu on 2015/4/29.
 */
public class BytewisePrefixComparator implements Comparator {

    public BytewisePrefixComparator() {}

    @Override
    public int compare(Slice a, Slice b) {
        return a.compareTo(b);
    }

    @Override
    public String name() {
        return "leveldb.BytewisePrefixComparator";
    }

    @Override
    public Slice findShortestSeparator(Slice start, Slice limit) {
        // not modify prefix
       return start;
    }

    @Override
    public Slice findShortSuccessor(Slice key) {
        // not modify prefix
        return key;
    }
}
