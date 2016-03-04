package com.github.zxl0714.leveldb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by xiaolu on 4/30/15.
 */
public class TestRandom {

    @Test
    public void testSimple() {
        Random r = new Random(302);
        assertEquals(r.next(), 5075714);
    }
}
