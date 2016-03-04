package com.github.zxl0714.leveldb;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by Xiaolu on 2015/4/28.
 */
public class TestCoding {

    @Test
    public void testDecodeFixed32() {
        Assert.assertEquals(Coding.decodeFixed32(new byte[]{(byte) 0xfe, (byte) 0x34, (byte) 0x12, (byte) 0xab}, 0), 0xab1234fe);
        Assert.assertEquals(Coding.decodeFixed32(new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}, 0), 0xffffffff);
        Assert.assertEquals(Coding.decodeFixed32(new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}, 0), 0x00000000);
    }

    @Test
    public void testEncodeFixed32() {
        Assert.assertArrayEquals(Coding.encodeFixed32(0xab1234fe), new byte[]{(byte) 0xfe, (byte) 0x34, (byte) 0x12, (byte) 0xab});
        Assert.assertArrayEquals(Coding.encodeFixed32(0xffffffff), new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
        Assert.assertArrayEquals(Coding.encodeFixed32(0x00000000), new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00});
    }

    @Test
    public void testDecodeFixed64() {
        Assert.assertEquals(Coding.decodeFixed64(new byte[]{(byte) 0xfe, (byte) 0x34, (byte) 0x12, (byte) 0xab,
                (byte) 0x11, (byte) 0x22, (byte) 0xab, (byte) 0xca}, 0), 0xcaab2211ab1234feL);
        Assert.assertEquals(Coding.decodeFixed64(new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}, 0), 0xffffffffffffffffL);
        Assert.assertEquals(Coding.decodeFixed64(new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}, 0), 0x0000000000000000L);
    }

    @Test
    public void testEncodeFixed64() {
        Assert.assertArrayEquals(Coding.encodeFixed64(0xcaab2211ab1234feL), new byte[]{(byte) 0xfe, (byte) 0x34, (byte) 0x12, (byte) 0xab,
                (byte) 0x11, (byte) 0x22, (byte) 0xab, (byte) 0xca});
        Assert.assertArrayEquals(Coding.encodeFixed64(0xffffffffffffffffL), new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
        Assert.assertArrayEquals(Coding.encodeFixed64(0x0000000000000000L), new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00});
    }

    @Test
    public void testVarint32() throws IOException {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        Coding.putVarint32(out, 0x4d);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x4d});
        out = ByteStreams.newDataOutput();
        Coding.putVarint32(out, 0x987);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x87, (byte) 0x13});
        out = ByteStreams.newDataOutput();
        Coding.putVarint32(out, 0xa987);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x87, (byte) 0xd3, (byte) 0x02});
        out = ByteStreams.newDataOutput();
        Coding.putVarint32(out, 0xf0a987);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x87, (byte) 0xd3, (byte) 0xc2, (byte) 0x07});
        out = ByteStreams.newDataOutput();
        Coding.putVarint32(out, 0xfedca987);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x87, (byte) 0xd3, (byte) 0xf2, (byte) 0xf6, (byte) 0x0f});
    }

    @Test
    public void testVarint64() throws IOException {
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        Coding.putVarint64(out, 0x4dL);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x4d});
        out = ByteStreams.newDataOutput();
        Coding.putVarint64(out, 0x987L);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x87, (byte) 0x13});
        out = ByteStreams.newDataOutput();
        Coding.putVarint64(out, 0xa987L);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x87, (byte) 0xd3, (byte) 0x02});
        out = ByteStreams.newDataOutput();
        Coding.putVarint64(out, 0xf0a987L);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x87, (byte) 0xd3, (byte) 0xc2, (byte) 0x07});
        out = ByteStreams.newDataOutput();
        Coding.putVarint64(out, 0xfedca987L);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0x87, (byte) 0xd3, (byte) 0xf2, (byte) 0xf6, (byte) 0x0f});
        out = ByteStreams.newDataOutput();
        Coding.putVarint64(out, 0xffffffffffffffffL);
        assertArrayEquals(out.toByteArray(), new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x01});
    }
}
