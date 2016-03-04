package com.github.zxl0714.leveldb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by xiaolu on 4/30/15.
 */
public class TestTableBuilder {

    private static Options options;

    public static Slice reverse(Slice key) {
        byte[] r = new byte[key.size()];
        for (int i = key.size() - 1; i >= 0; i--) {
            r[key.size() - 1 - i] = key.data()[i];
        }
        return new Slice(r);
    }

    public static class ReverseKeyComparator implements Comparator {

        private BytewiseComparator cmp = new BytewiseComparator();

        @Override
        public int compare(Slice a, Slice b) {
            return cmp.compare(reverse(a), reverse(b));
        }

        @Override
        public String name() {
            return "leveldb.ReverseBytewiseComparator";
        }

        @Override
        public Slice findShortestSeparator(Slice start, Slice limit) {
            Slice s = reverse(start);
            Slice l = reverse(limit);
            return reverse(cmp.findShortestSeparator(s, l));
        }

        @Override
        public Slice findShortSuccessor(Slice key) {
            return reverse(cmp.findShortSuccessor(reverse(key)));
        }
    }

    public static class TestArgs {
        public boolean reverseCompare;
        public int restartInterval;
        public boolean useFilter;
        public TestArgs(boolean reverseCompare, int restartInterval, boolean useFilter) {
            this.reverseCompare = reverseCompare;
            this.restartInterval = restartInterval;
            this.useFilter = useFilter;
        }
    }

    public void init(TestArgs args) {
        options = new Options();
        options.compression = Options.CompressionType.NO_COMPRESSION;
        options.blockRestartInterval = args.restartInterval;
        if (args.reverseCompare) {
            options.comparator = new ReverseKeyComparator();
        }
        options.blockSize = 256;
        if (args.useFilter) {
            options.filterPolicy = new BloomFilterPolicy(10);
        }
    }

    public static class CompareKey implements java.util.Comparator<Map.Entry<Slice, Slice>> {

        private final Comparator cmp;

        public CompareKey(Comparator cmp) {
            this.cmp = cmp;
        }

        @Override
        public int compare(Map.Entry<Slice, Slice> v1, Map.Entry<Slice, Slice> v2) {
            return cmp.compare(v1.getKey(), v2.getKey());
        }
    }

    public String generateTable(Options options, Map<Slice, Slice> data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        TableBuilder builder = new TableBuilder(options, out);
        List<Map.Entry<Slice, Slice>> dataList = Lists.newArrayList();
        dataList.addAll(data.entrySet());
        Collections.sort(dataList, new CompareKey(options.comparator));
        for (Map.Entry<Slice, Slice> entry : dataList) {
            builder.add(entry.getKey(), entry.getValue());
        }
        builder.finish();
        byte[] result = out.toByteArray();
        assertEquals(result.length, builder.fileSize());
        return Utils.escapeString(new Slice(result));
    }

    @Test
    public void testEmpty() throws IOException {
        Map<Slice, Slice> data = Maps.newHashMap();
        init(new TestArgs(false, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testEmpty.in")));
        init(new TestArgs(false, 1, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testEmpty.in")));
        init(new TestArgs(false, 1024, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testEmpty.in")));
        init(new TestArgs(true, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testEmpty.in")));
        init(new TestArgs(true, 1, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testEmpty.in")));
        init(new TestArgs(true, 1024, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testEmpty.in")));
    }

    @Test
    public void testSimpleEmptyKey() throws IOException {
        Map<Slice, Slice> data = Maps.newHashMap();
        data.put(new Slice(""), new Slice("v"));
        init(new TestArgs(false, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleEmptyKey.in")));
        init(new TestArgs(false, 1, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleEmptyKey.in")));
        init(new TestArgs(false, 1024, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleEmptyKey.in")));
        init(new TestArgs(true, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleEmptyKey.in")));
        init(new TestArgs(true, 1, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleEmptyKey.in")));
        init(new TestArgs(true, 1024, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleEmptyKey.in")));
    }

    @Test
    public void testSimpleMulti() throws IOException {
        Map<Slice, Slice> data = Maps.newTreeMap();
        data.put(new Slice("abc"), new Slice("v"));
        data.put(new Slice("abcd"), new Slice("v"));
        data.put(new Slice("ac"), new Slice("v2"));
        init(new TestArgs(false, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleMulti.in1")));
        init(new TestArgs(false, 1, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleMulti.in2")));
        init(new TestArgs(false, 1024, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleMulti.in1")));
        init(new TestArgs(true, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleMulti.in3")));
        init(new TestArgs(true, 1, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleMulti.in4")));
        init(new TestArgs(true, 1024, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleMulti.in3")));
    }

    @Test
    public void testSimpleSpecialKey() throws IOException {
        Map<Slice, Slice> data = Maps.newTreeMap();
        data.put(new Slice(new byte[]{(byte) 0xff, (byte) 0xff}), new Slice("v3"));
        init(new TestArgs(false, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleSpecialKey.in")));
        init(new TestArgs(true, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testSimpleSpecialKey.in")));
    }

    @Test
    public void testRandomized() throws IOException {
        Map<Slice, Slice> data = Maps.newTreeMap();
        Random rnd = new Random(306);
        for (int i = 0; i < 1000; i++) {
            Slice key = Utils.randomKey(rnd, rnd.skewed(4));
            Slice value = Utils.randomString(rnd, rnd.skewed(5));
            data.put(key, value);
        }
        init(new TestArgs(false, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testRandomized.in1")));
        init(new TestArgs(false, 1, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testRandomized.in2")));
        init(new TestArgs(false, 1024, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testRandomized.in3")));
        init(new TestArgs(true, 16, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testRandomized.in4")));
        init(new TestArgs(true, 1, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testRandomized.in5")));
        init(new TestArgs(true, 1024, false));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testRandomized.in6")));
    }

    @Test
    public void testTableWithBloomFilterSimple() throws IOException {
        Map<Slice, Slice> data = Maps.newHashMap();
        data.put(new Slice("abc"), new Slice("v"));
        init(new TestArgs(false, 16, true));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testTableWithBloomFilterSimple.in")));
    }

    @Test
    public void testTableWithBloomFilterRandomized() throws IOException {
        Map<Slice, Slice> data = Maps.newTreeMap();
        Random rnd = new Random(306);
        for (int i = 0; i < 1000; i++) {
            Slice key = Utils.randomKey(rnd, rnd.skewed(4));
            Slice value = Utils.randomString(rnd, rnd.skewed(5));
            data.put(key, value);
        }
        init(new TestArgs(false, 16, true));
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testTableWithBloomFilterRandomized.in")));
    }

    @Test
    public void testWeirdTable() throws IOException {
        Map<Slice, Slice> data = Maps.newTreeMap();
        data.put(new Slice("k01"), new Slice("hello"));
        data.put(new Slice("k02"), new Slice("hello2"));
        byte[] v1 = new byte[10000];
        Arrays.fill(v1, (byte) 'x');
        data.put(new Slice("k03"), new Slice(v1));
        byte[] v2 = new byte[200000];
        Arrays.fill(v2, (byte) 'x');
        data.put(new Slice("k04"), new Slice(v2));
        byte[] v3 = new byte[300000];
        Arrays.fill(v3, (byte) 'x');
        data.put(new Slice("k05"), new Slice(v3));
        data.put(new Slice("k06"), new Slice("hello3"));
        byte[] v4 = new byte[100000];
        Arrays.fill(v4, (byte) 'x');
        data.put(new Slice("k07"), new Slice(v4));
        Options options = new Options();
        options.blockSize = 1024;
        options.compression = Options.CompressionType.NO_COMPRESSION;
        Assert.assertEquals(generateTable(options, data), Utils.readFile(TestTableBuilder.class.getResource("TableTest.testWeirdTable.in")));
    }
}
