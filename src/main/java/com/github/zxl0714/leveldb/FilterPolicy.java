package com.github.zxl0714.leveldb;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Created by Xiaolu on 2015/4/28.
 */
public interface FilterPolicy {
    public String name();
    public int createFilter(List<Slice> keys, DataOutput dst) throws IOException;
}
