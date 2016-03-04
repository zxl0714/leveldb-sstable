package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Created by xiaolu on 4/28/15.
 */
public class DBFormat {

    public static long packSequenceAndType(long seq, int type) {
        Preconditions.checkArgument(seq <= MAX_SEQUENCE_NUMBER);
        Preconditions.checkArgument(type <= 1);

        return (seq << 8) | type;
    }

    public static class ValueType {
        public static int TYPE_DELETION = 0x0;
        public static int TYPE_VALUE = 0x1;
    }

    public static int VALUE_TYPE_FOR_SEEK = ValueType.TYPE_VALUE;
    public static long MAX_SEQUENCE_NUMBER = (1L << 56) - 1;

    public static Slice extractUserKey(Slice internalKey) {
        Preconditions.checkNotNull(internalKey);
        Preconditions.checkArgument(internalKey.size() >= 8);

        return new Slice(internalKey.data(), internalKey.size() - 8);
    }

    public static class InternalKey {

        private Slice data = null;

        public InternalKey(Slice userKey, long sequenceNumber, int valueType) throws IOException {
            ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
            buffer.write(userKey.data(), 0, userKey.size());
            Coding.putFixed64(buffer, packSequenceAndType(sequenceNumber, valueType));
            data = new Slice(buffer.toByteArray());
        }

        public Slice userKey() {
            return extractUserKey(data);
        }

        public Slice encode() {
            return data;
        }
    }

    public static class InternalKeyComparator implements Comparator {

        private final Comparator userComparator;

        public InternalKeyComparator(Comparator userComparator) {
            Preconditions.checkNotNull(userComparator);

            this.userComparator = userComparator;
        }

        @Override
        public int compare(Slice a, Slice b) {
            int r = userComparator.compare(extractUserKey(a), extractUserKey(b));
            if (r == 0) {
                long aNum = Coding.decodeFixed64(a.data(), a.size() - 8);
                long bNum = Coding.decodeFixed64(b.data(), b.size() - 8);
                if (aNum > bNum) {
                    r = -1;
                } else {
                    r = +1;
                }
            }
            return r;
        }

        @Override
        public String name() {
            return "leveldb.InternalKeyComparator";
        }

        @Override
        public Slice findShortestSeparator(Slice start, Slice limit) {
            Slice userStart = extractUserKey(start);
            Slice userLimit = extractUserKey(limit);
            Slice tmp = userComparator.findShortestSeparator(userStart, userLimit);
            if (tmp.size() < userStart.size() && userComparator.compare(userStart, tmp) < 0) {
                ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
                buffer.write(tmp.data(), 0, tmp.size());
                try {
                    Coding.putFixed64(buffer, packSequenceAndType(MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK));
                } catch (IOException e) {
                    // should never exception
                    return start;
                }
                tmp = new Slice(buffer.toByteArray());
                Preconditions.checkState(compare(start, tmp) < 0);
                Preconditions.checkState(compare(tmp, limit) < 0);
                return tmp;
            }
            return start;
        }

        @Override
        public Slice findShortSuccessor(Slice key) {
            Slice userKey = extractUserKey(key);
            Slice tmp = userComparator.findShortSuccessor(userKey);
            if (tmp.size() < userKey.size() && userComparator.compare(userKey, tmp) < 0) {
                ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
                buffer.write(tmp.data(), 0, tmp.size());
                try {
                    Coding.putFixed64(buffer, packSequenceAndType(MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK));
                } catch (IOException e) {
                    // should never exception
                    return key;
                }
                tmp = new Slice(buffer.toByteArray());
                Preconditions.checkState(compare(key, tmp) < 0);
                return tmp;
            }
            return key;
        }
    }

    public static class InternalFilterPolicy implements FilterPolicy {

        private final FilterPolicy userPolicy;

        public InternalFilterPolicy(FilterPolicy userPolicy) {
            Preconditions.checkNotNull(userPolicy);

            this.userPolicy = userPolicy;
        }

        @Override
        public String name() {
            return userPolicy.name();
        }

        @Override
        public int createFilter(List<Slice> keys, DataOutput dst) throws IOException {
            Preconditions.checkNotNull(keys);

            ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<Slice>();
            for (Slice key : keys) {
                builder.add(extractUserKey(key));
            }
            return userPolicy.createFilter(builder.build(), dst);
        }
    }
}
