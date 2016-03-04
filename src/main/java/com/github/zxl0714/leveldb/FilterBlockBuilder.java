package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.util.List;

/**
 * Created by Xiaolu on 2015/4/28.
 */
public class FilterBlockBuilder {

    private static final int FILTER_BASE_LG = 11;
    private static final int FILTER_BASE = 1 << FILTER_BASE_LG;

    private final FilterPolicy policy;
    private final List<Slice> keys = Lists.newArrayList();
    private final List<Integer> filterOffsets = Lists.newArrayList();
    private final ByteArrayDataOutput result = ByteStreams.newDataOutput();

    private int arrayOffset = 0;

    public FilterBlockBuilder(FilterPolicy policy) {
        this.policy = policy;
    }

    public void startBlock(int blockOffset) throws IOException {
        int filterIndex = blockOffset / FILTER_BASE;
        Preconditions.checkArgument(filterIndex >= filterOffsets.size());
        while (filterIndex > filterOffsets.size()) {
            generateFilter();
        }
    }

    public void addKey(Slice key) {
        keys.add(key);
    }

    public Slice finish() throws IOException {
        if (!keys.isEmpty()) {
            generateFilter();
        }

        // Append array of per-filter offsets
        for (int offset : filterOffsets) {
            Coding.putFixed32(result, offset);
        }

        Coding.putFixed32(result, arrayOffset);
        result.writeByte(FILTER_BASE_LG);
        return new Slice(result.toByteArray());
    }

    private void generateFilter() throws IOException {
        if (keys.isEmpty()) {
            // Fast path if there are no keys for this filter
            filterOffsets.add(arrayOffset);
            return ;
        }

        filterOffsets.add(arrayOffset);
        arrayOffset += policy.createFilter(keys, result);

        keys.clear();
    }
}
