package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by xiaolu on 5/5/15.
 */
public class SSTableWriter implements Closeable {

    private final OutputStream out;
    private final TableBuilder tableBuilder;

    private Options options;
    private DBFormat.InternalKey firstKey = null;
    private DBFormat.InternalKey lastKey = null;

    public SSTableWriter(Options options, OutputStream out) throws IOException {
        Preconditions.checkNotNull(options);
        Preconditions.checkNotNull(out);

        this.options = new Options(options);
        if (options.comparator != null) {
            this.options.comparator = new DBFormat.InternalKeyComparator(options.comparator);
        }
        if (options.filterPolicy != null) {
            this.options.filterPolicy = new DBFormat.InternalFilterPolicy(options.filterPolicy);
        }
        this.out = out;
        this.tableBuilder = new TableBuilder(this.options, out);
    }

    public void add(Slice key, Slice value) throws IOException {
        DBFormat.InternalKey ikey = new DBFormat.InternalKey(key, 0, DBFormat.ValueType.TYPE_VALUE);
        tableBuilder.add(ikey.encode(), value);
        if (firstKey == null) {
            firstKey = ikey;
        }
        lastKey = ikey;
    }

    public void finish() throws IOException {
        tableBuilder.finish();
    }

    public long getFileSize() {
        return tableBuilder.fileSize();
    }

    public DBFormat.InternalKey getFirstKey() {
        return firstKey;
    }

    public DBFormat.InternalKey getLastKey() {
        return lastKey;
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}
