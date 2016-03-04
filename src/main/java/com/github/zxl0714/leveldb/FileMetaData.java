package com.github.zxl0714.leveldb;

/**
 * Created by xiaolu on 5/4/15.
 */
public class FileMetaData {

    private long number = 0;
    private long fileSize = 0;
    private DBFormat.InternalKey smallest = null;
    private DBFormat.InternalKey largest = null;

    public DBFormat.InternalKey getLargest() {
        return largest;
    }

    public void setLargest(DBFormat.InternalKey largest) {
        this.largest = largest;
    }

    public long getNumber() {
        return number;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public DBFormat.InternalKey getSmallest() {
        return smallest;
    }

    public void setSmallest(DBFormat.InternalKey smallest) {
        this.smallest = smallest;
    }
}
