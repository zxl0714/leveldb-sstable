package com.github.zxl0714.leveldb;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by xiaolu on 5/4/15.
 */
public class TestDBFormat {

    @Test
    public void testInternalKeyShortSeparator() throws IOException {
        DBFormat.InternalKeyComparator cmp = new DBFormat.InternalKeyComparator(new BytewiseComparator());

        assertArrayEquals(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode().data(),
                cmp.findShortestSeparator(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode(),
                        new DBFormat.InternalKey(new Slice("foo"), 99, DBFormat.ValueType.TYPE_VALUE).encode()).data());
        assertArrayEquals(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode().data(),
                cmp.findShortestSeparator(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode(),
                        new DBFormat.InternalKey(new Slice("foo"), 101, DBFormat.ValueType.TYPE_VALUE).encode()).data());
        assertArrayEquals(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode().data(),
                cmp.findShortestSeparator(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode(),
                        new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode()).data());
        assertArrayEquals(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode().data(),
                cmp.findShortestSeparator(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode(),
                        new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_DELETION).encode()).data());

        assertArrayEquals(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode().data(),
                cmp.findShortestSeparator(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode(),
                        new DBFormat.InternalKey(new Slice("bar"), 99, DBFormat.ValueType.TYPE_VALUE).encode()).data());

        assertArrayEquals(new DBFormat.InternalKey(new Slice("g"), DBFormat.MAX_SEQUENCE_NUMBER, DBFormat.VALUE_TYPE_FOR_SEEK).encode().data(),
                cmp.findShortestSeparator(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode(),
                        new DBFormat.InternalKey(new Slice("hello"), 200, DBFormat.ValueType.TYPE_VALUE).encode()).data());

        assertArrayEquals(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode().data(),
                cmp.findShortestSeparator(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode(),
                        new DBFormat.InternalKey(new Slice("foobar"), 200, DBFormat.ValueType.TYPE_VALUE).encode()).data());

        assertArrayEquals(new DBFormat.InternalKey(new Slice("foobar"), 100, DBFormat.ValueType.TYPE_VALUE).encode().data(),
                cmp.findShortestSeparator(new DBFormat.InternalKey(new Slice("foobar"), 100, DBFormat.ValueType.TYPE_VALUE).encode(),
                        new DBFormat.InternalKey(new Slice("foo"), 99, DBFormat.ValueType.TYPE_VALUE).encode()).data());
    }

    @Test
    public void testShortestSuccessor() throws IOException {
        DBFormat.InternalKeyComparator cmp = new DBFormat.InternalKeyComparator(new BytewiseComparator());

        assertArrayEquals(new DBFormat.InternalKey(new Slice("g"), DBFormat.MAX_SEQUENCE_NUMBER, DBFormat.VALUE_TYPE_FOR_SEEK).encode().data(),
                cmp.findShortSuccessor(new DBFormat.InternalKey(new Slice("foo"), 100, DBFormat.ValueType.TYPE_VALUE).encode()).data());
        assertArrayEquals(new DBFormat.InternalKey(new Slice(new byte[]{(byte) 0xff, (byte) 0xff}), 100, DBFormat.ValueType.TYPE_VALUE).encode().data(),
                cmp.findShortSuccessor(new DBFormat.InternalKey(new Slice(new byte[]{(byte) 0xff, (byte) 0xff}), 100, DBFormat.ValueType.TYPE_VALUE).encode()).data());
    }
}
