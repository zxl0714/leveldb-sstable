package com.github.zxl0714.leveldb;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Xiaolu on 2015/4/29.
 */
public class TestBloomFilter {

    @Test
    public void TestEmptyBuilder() throws IOException {
        FilterPolicy bloom = new BloomFilterPolicy(10);
        ImmutableList.Builder<Slice> keys = new ImmutableList.Builder<Slice>();
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        bloom.createFilter(keys.build(), out);
        assertEquals(Utils.escapeString(new Slice(out.toByteArray())), "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x06");
    }

    @Test
    public void TestSimpleBuilder() throws IOException {
        FilterPolicy bloom = new BloomFilterPolicy(10);
        ImmutableList.Builder<Slice> keys = new ImmutableList.Builder<Slice>();
        keys.add(new Slice("foo"));
        keys.add(new Slice("bar"));
        keys.add(new Slice("leveldb"));
        keys.add(new Slice("mapreduce"));
        keys.add(new Slice(""));
        keys.add(new Slice("1234"));
        keys.add(new Slice("\0\1\2\3\4"));
        keys.add(new Slice(new byte[]{(byte) 255, (byte) 254, (byte) 253, (byte) 252, (byte) 251}));
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        bloom.createFilter(keys.build(), out);
        assertEquals(Utils.escapeString(new Slice(out.toByteArray())), "4\\xfd_1C\\x96<\\x18\\x12K\\x06");
    }
}
