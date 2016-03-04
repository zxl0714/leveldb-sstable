package com.github.zxl0714.leveldb;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Created by xiaolu on 4/29/15.
 */
public class TestHash {

    @Test
    public void testHash() {
        byte[] data1 = new byte[]{(byte) 0x62};
        byte[] data2 = new byte[]{(byte) 0xc3, (byte) 0x97};
        byte[] data3 = new byte[]{(byte) 0xe2, (byte) 0x99, (byte) 0xa5};
        byte[] data4 = new byte[]{(byte) 0xe1, (byte) 0x80, (byte) 0xb9, (byte) 0x32};
        byte[] data5 = new byte[]{
                0x01, (byte) 0xc0, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x04, 0x00,
                0x00, 0x00, 0x00, 0x14,
                0x00, 0x00, 0x00, 0x18,
                0x28, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
        };
        assertEquals(Hash.hash(new byte[0], 0, 0xbc9f1d34), 0xbc9f1d34);
        assertEquals(Hash.hash(data1, data1.length, 0xbc9f1d34), 0xef1345c4);
        assertEquals(Hash.hash(data2, data2.length, 0xbc9f1d34), 0x5b663814);
        assertEquals(Hash.hash(data3, data3.length, 0xbc9f1d34), 0x323c078f);
        assertEquals(Hash.hash(data4, data4.length, 0xbc9f1d34), 0xed21633a);
        assertEquals(Hash.hash(data5, data5.length, 0x12345678), 0xf333dabb);
    }

    @Test
    public void testCrc32c() {
        byte[] buf = new byte[32];
        Arrays.fill(buf, (byte) 0);
        assertEquals(0x8a9136aa, Crc32c.value(buf, buf.length));
        Arrays.fill(buf, (byte) 0xff);
        assertEquals(0x62a8ab43, Crc32c.value(buf, buf.length));
        for (int i = 0; i < 32; i++) {
            buf[i] = (byte) i;
        }
        assertEquals(0x46dd794e, Crc32c.value(buf, buf.length));
        String hello = "hello ";
        String world = "world";
        assertEquals(Crc32c.value((hello + world).getBytes(), (hello + world).length()),
                Crc32c.extend(Crc32c.value(hello.getBytes(), hello.length()), world.getBytes(), 0, world.length()));
        assertEquals(Crc32c.mask(Crc32c.value("foo".getBytes(), 3)), 0xfebe8a61);
    }
}
