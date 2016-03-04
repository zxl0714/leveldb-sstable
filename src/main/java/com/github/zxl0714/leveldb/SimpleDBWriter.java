package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by xiaolu on 5/5/15.
 */
public class SimpleDBWriter implements Closeable {

    private final Options options;
    private final String outputDir;
    private final long limitTableSize;
    private final VersionEdit edit;

    private SSTableWriter current = null;
    private int lastFileNumber = 10;

    public SimpleDBWriter(Options options, String outputDir, long limitTableSize) throws IOException {
        this.options = new Options(options);
        this.outputDir = outputDir;
        this.limitTableSize = limitTableSize;
        this.edit = new VersionEdit();
        if (options.comparator != null) {
            this.edit.setComparatorName(options.comparator.name());
        }
        this.edit.setLastSequence(1);
        this.edit.setPrevLogNumber(0);
        if (!Files.exists(Paths.get(outputDir))) {
            Files.createDirectory(Paths.get(outputDir));
        }
    }

    public void add(Slice key, Slice value) throws IOException {
        if (current == null || current.getFileSize() > limitTableSize) {
            finishCurrent();
            openNewSSTable();
        }
        current.add(key, value);
    }

    private void finishCurrent() throws IOException {
        if (current == null) {
            return ;
        }
        current.finish();
        if (current.getFileSize() != 0) {
            edit.addFile(1, lastFileNumber, current.getFileSize(), current.getFirstKey(), current.getLastKey());
        }
        current.close();
        current = null;
    }

    private String newFileNameForSSTable() {
        return String.format("%06d.ldb", ++lastFileNumber);
    }

    private void openNewSSTable() throws IOException {
        Preconditions.checkState(current == null);

        current = new SSTableWriter(options, new FileOutputStream(Paths.get(outputDir, newFileNameForSSTable()).toString()));
    }

    private void writeCurrentFile() throws IOException {
        OutputStream out = null;
        try {
            out = new FileOutputStream(Paths.get(outputDir, "CURRENT").toString());
            IOUtils.write("MANIFEST-000001\n", out);
            out.flush();
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    private void writeLockFile() throws IOException {
        OutputStream out = null;
        try {
            out = new FileOutputStream(Paths.get(outputDir, "LOCK").toString());
            out.flush();
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    private void writeManifestFile() throws IOException {
        LogWriter logWriter = null;
        try {
            logWriter = new LogWriter(new FileOutputStream(Paths.get(outputDir, "MANIFEST-000001").toString()));
            edit.setLogNumber(lastFileNumber + 1);
            edit.setNextFile(lastFileNumber + 2);
            ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
            edit.encodeTo(buffer);
            logWriter.addRecord(new Slice(buffer.toByteArray()));
            logWriter.flush();
        } finally {
            IOUtils.closeQuietly(logWriter);
        }
    }

    @Override
    public void close() throws IOException {
        finishCurrent();
        writeCurrentFile();
        writeLockFile();
        writeManifestFile();
    }
}
