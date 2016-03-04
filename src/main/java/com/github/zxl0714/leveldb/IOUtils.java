package com.github.zxl0714.leveldb;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public final class IOUtils {

    public static void closeQuietly(Closeable c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (IOException e) {
            // ignored
        }
    }

    public static void write(String s, OutputStream out) throws IOException {
        out.write(s.getBytes("UTF-8"));
    }
}
