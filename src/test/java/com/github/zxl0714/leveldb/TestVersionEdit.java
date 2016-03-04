package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by xiaolu on 5/4/15.
 */
public class TestVersionEdit {

    private static class VersionEditTest {

        private final VersionEdit edit;

        public VersionEditTest(VersionEdit edit) {
            Preconditions.checkNotNull(edit);

            this.edit = edit;
        }

        public void testEncode() throws IOException {
            ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
            edit.encodeTo(buffer);
            byte[] data = buffer.toByteArray();
            InputStream in = new ByteArrayInputStream(data);
            if (edit.getComparator() != null) {
                expectVarint32(in, VersionEdit.Tag.COMPARATOR);
                expectVarint32(in, edit.getComparator().length());
                expectBytes(in, edit.getComparator().getBytes());
            }
            if (edit.getLogNumber() != null) {
                expectVarint32(in, VersionEdit.Tag.LOG_NUMBER);
                expectVarint64(in, edit.getLogNumber());
            }
            if (edit.getPrevLogNumber() != null) {
                expectVarint32(in, VersionEdit.Tag.PREV_LOG_NUMBER);
                expectVarint64(in, edit.getPrevLogNumber());
            }
            if (edit.getNextFileNumber() != null) {
                expectVarint32(in, VersionEdit.Tag.NEXT_FILE_NUMBER);
                expectVarint64(in, edit.getNextFileNumber());
            }
            if (edit.getLastSequence() != null) {
                expectVarint32(in, VersionEdit.Tag.LAST_SEQUENCE);
                expectVarint64(in, edit.getLastSequence());
            }
            for (Map.Entry<Integer, FileMetaData> entry : edit.getNewFiles().entries()) {
                FileMetaData f = entry.getValue();
                expectVarint32(in, VersionEdit.Tag.NEW_FILE);
                expectVarint32(in, entry.getKey());
                expectVarint64(in, f.getNumber());
                expectVarint64(in, f.getFileSize());
                expectVarint32(in, f.getSmallest().encode().size());
                expectBytes(in, f.getSmallest().encode().data());
                expectVarint32(in, f.getLargest().encode().size());
                expectBytes(in, f.getLargest().encode().data());
            }
            expectEOF(in);
        }

        public static void expectVarint32(InputStream in, int value) throws IOException {
            ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
            Coding.putVarint32(buffer, value);
            expectBytes(in, buffer.toByteArray());
        }

        public static void expectVarint64(InputStream in, long value) throws IOException {
            ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
            Coding.putVarint64(buffer, value);
            expectBytes(in, buffer.toByteArray());
        }

        public static void expectBytes(InputStream in, byte[] value) throws IOException {
            byte[] r = new byte[value.length];
            assertTrue(in.read(r) != -1);
            assertArrayEquals(r, value);
        }
        public static void expectEOF(InputStream in) throws IOException {
            assertTrue(in.read() == -1);
        }
    }

    @Test
    public void testEncode() throws IOException {
        VersionEdit edit = new VersionEdit();
        VersionEditTest test = new VersionEditTest(edit);
        long big = 1l << 50;
        for (int i = 0; i < 4; i++) {
            test.testEncode();
            edit.addFile(3, big + 300 + i, big + 400 + i,
                    new DBFormat.InternalKey(new Slice("foo"), big + 500 + i, DBFormat.ValueType.TYPE_VALUE),
                    new DBFormat.InternalKey(new Slice("zoo"), big + 600 + i, DBFormat.ValueType.TYPE_DELETION));
        }
        edit.setComparatorName("foo");
        edit.setLogNumber(big + 100);
        edit.setNextFile(big + 200);
        edit.setLastSequence(big + 1000);
        test.testEncode();
    }
}
