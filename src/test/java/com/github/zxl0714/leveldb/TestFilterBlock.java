package com.github.zxl0714.leveldb;

import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static com.github.zxl0714.leveldb.Utils.escapeString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by Xiaolu on 2015/4/29.
 */
public class TestFilterBlock {

    class TestHashFilter implements FilterPolicy {

        @Override
        public String name() {
            return "TestHashFilter";
        }

        @Override
        public int createFilter(List<Slice> keys, DataOutput dst) throws IOException {
            for (Slice key : keys) {
                int h = Hash.hash(key.data(), key.size(), 1);
                Coding.putFixed32(dst, h);
            }
            return keys.size() * 4;
        }
    }

    @Test
    public void testEmptyBuilder() throws IOException {
        FilterBlockBuilder builder = new FilterBlockBuilder(new TestHashFilter());
        Slice block = builder.finish();
        assertEquals(escapeString(block), "\\x00\\x00\\x00\\x00\\x0b");
    }

    @Test
    public void testSingleChunk() throws IOException {
        FilterBlockBuilder builder = new FilterBlockBuilder(new TestHashFilter());
        builder.startBlock(100);
        builder.addKey(new Slice("foo"));
        builder.addKey(new Slice("bar"));
        builder.addKey(new Slice("box"));
        builder.startBlock(200);
        builder.addKey(new Slice("box"));
        builder.startBlock(300);
        builder.addKey(new Slice("hello"));
        Slice block = builder.finish();
        assertEquals(escapeString(block), "\\xc45w\\xfe7\\x8cs\\xd9&\\x96\\x0f\\xc8&\\x96\\x0f\\xc8\\xbf'\\x8b\\x0e\\x00\\x00\\x00\\x00\\x14\\x00\\x00\\x00\\x0b");
    }

    @Test
    public void testMultiChunk() throws IOException {
        FilterBlockBuilder builder = new FilterBlockBuilder(new TestHashFilter());

        // First filter
        builder.startBlock(0);
        builder.addKey(new Slice("foo"));
        builder.startBlock(2000);
        builder.addKey(new Slice("bar"));

        // Second filter
        builder.startBlock(3100);
        builder.addKey(new Slice("box"));

        // Third filter is empty

        // Last filter
        builder.startBlock(9000);
        builder.addKey(new Slice("box"));
        builder.addKey(new Slice("hello"));

        Slice block = builder.finish();
        assertEquals(escapeString(block), "\\xc45w\\xfe7\\x8cs\\xd9&\\x96\\x0f\\xc8&\\x96\\x0f\\xc8\\xbf'\\x8b\\x0e\\x00\\x00\\x00\\x00\\x08\\x00\\x00\\x00\\x0c\\x00\\x00\\x00\\x0c\\x00\\x00\\x00\\x0c\\x00\\x00\\x00\\x14\\x00\\x00\\x00\\x0b");
    }
}
