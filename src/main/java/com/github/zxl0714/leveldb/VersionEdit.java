package com.github.zxl0714.leveldb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * Created by xiaolu on 5/4/15.
 */
public class VersionEdit {

    private String comparator;
    private Long logNumber;
    private Long prevLogNumber;
    private Long nextFileNumber;
    private Long lastSequence;
    private Multimap<Integer, FileMetaData> newFiles;

    public void addFile(int level,
                        long file,
                        long fileSize,
                        DBFormat.InternalKey smallest,
                        DBFormat.InternalKey largest) {
        FileMetaData f = new FileMetaData();
        f.setNumber(file);
        f.setFileSize(fileSize);
        f.setSmallest(smallest);
        f.setLargest(largest);
        newFiles.put(level, f);
    }

    public void setComparatorName(String name) {
        Preconditions.checkNotNull(name);

        this.comparator = name;
    }

    public void setLogNumber(long logNumber) {
        this.logNumber = logNumber;
    }

    public void setPrevLogNumber(long prevLogNumber) {
        this.prevLogNumber = prevLogNumber;
    }

    public void setNextFile(long nextFileNumber) {
        this.nextFileNumber = nextFileNumber;
    }

    public void setLastSequence(long lastSequence) {
        this.lastSequence = lastSequence;
    }

    public VersionEdit() {
        clear();
    }

    public void clear() {
        comparator = null;
        logNumber = null;
        prevLogNumber = null;
        nextFileNumber = null;
        lastSequence = null;
        newFiles = ArrayListMultimap.create();
    }

    @VisibleForTesting String getComparator() {
        return comparator;
    }

    @VisibleForTesting Long getLogNumber() {
        return logNumber;
    }

    @VisibleForTesting Long getPrevLogNumber() {
        return prevLogNumber;
    }

    @VisibleForTesting Long getNextFileNumber() {
        return nextFileNumber;
    }

    @VisibleForTesting Long getLastSequence() {
        return lastSequence;
    }

    @VisibleForTesting Multimap<Integer, FileMetaData> getNewFiles() {
        return newFiles;
    }

    public static class Tag {
        public static final int COMPARATOR = 1;
        public static final int LOG_NUMBER = 2;
        public static final int NEXT_FILE_NUMBER = 3;
        public static final int LAST_SEQUENCE = 4;
        public static final int COMPACT_POINTER = 5;
        public static final int DELETED_FILE = 6;
        public static final int NEW_FILE = 7;
        public static final int PREV_LOG_NUMBER = 9;
    }

    public void encodeTo(DataOutput dst) throws IOException {
        if (comparator != null) {
            Coding.putVarint32(dst, Tag.COMPARATOR);
            Coding.putLengthPrefixedSlice(dst, new Slice(comparator));
        }
        if (logNumber != null) {
            Coding.putVarint32(dst, Tag.LOG_NUMBER);
            Coding.putVarint64(dst, logNumber);
        }
        if (prevLogNumber != null) {
            Coding.putVarint32(dst, Tag.PREV_LOG_NUMBER);
            Coding.putVarint64(dst, prevLogNumber);
        }
        if (nextFileNumber != null) {
            Coding.putVarint32(dst, Tag.NEXT_FILE_NUMBER);
            Coding.putVarint64(dst, nextFileNumber);
        }
        if (lastSequence != null) {
            Coding.putVarint32(dst, Tag.LAST_SEQUENCE);
            Coding.putVarint64(dst, lastSequence);
        }

        // we don't need compact pointers and deleted file

        for (Map.Entry<Integer, FileMetaData> entry : newFiles.entries()) {
            FileMetaData f = entry.getValue();
            Coding.putVarint32(dst, Tag.NEW_FILE);
            Coding.putVarint32(dst, entry.getKey());
            Coding.putVarint64(dst, f.getNumber());
            Coding.putVarint64(dst, f.getFileSize());
            Coding.putLengthPrefixedSlice(dst, f.getSmallest().encode());
            Coding.putLengthPrefixedSlice(dst, f.getLargest().encode());
        }
    }
}
