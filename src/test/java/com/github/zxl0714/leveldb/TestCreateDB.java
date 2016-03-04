package com.github.zxl0714.leveldb;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Created by xiaolu on 5/5/15.
 */
public class TestCreateDB {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void testSimpleDBWriter() throws IOException {
        Options options = new Options();
        options.filterPolicy = new BloomFilterPolicy(10);
        SimpleDBWriter writer = new SimpleDBWriter(options, tmpDir.newFolder("test_build_db").toString(), 1024 * 8);
        for (int i = 0; i < 10000; i += 2) {
            writer.add(new Slice(String.format("%06d", i)), new Slice(String.valueOf(i)));
        }
        writer.close();
    }
}
