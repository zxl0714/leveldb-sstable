package com.github.zxl0714.leveldb;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Created by Xiaolu on 2015/4/29.
 */
public class Utils {

    private static final byte[] kTestChars = new byte[]{0, 1, 'a', 'b', 'c', 'd', 'e', (byte) 0xfd, (byte) 0xfe, (byte) 0xff};

    public static String escapeString(Slice value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.size(); i++) {
            char c = (char) value.data()[i];
            if (c >= ' ' && c <= '~') {
                builder.append(c);
            } else {
                builder.append(String.format("\\x%02x", c & 0xff));
            }
        }
        return builder.toString();
    }

    public static Slice randomKey(Random rnd, int len) {
        byte[] ret = new byte[len];
        for (int i = 0; i < len; i++) {
            ret[i] = kTestChars[rnd.uniform(kTestChars.length)];
        }
        return new Slice(ret);
    }

    public static Slice randomString(Random rnd, int len) {
        byte[] ret = new byte[len];
        for (int i = 0; i < len; i++) {
            ret[i] = (byte) (' ' + rnd.uniform(95));
        }
        return new Slice(ret);
    }

    public static String readFile(URL url) throws IOException {
        InputStream in = url.openStream();
        ByteArrayDataOutput bout = ByteStreams.newDataOutput();
        int b;
        while ((b = in.read()) != -1) {
            bout.writeByte(b);
        }
        in.close();
        return new String(bout.toByteArray());
    }
}
