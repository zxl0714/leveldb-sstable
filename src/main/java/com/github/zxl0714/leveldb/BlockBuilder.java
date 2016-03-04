package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.util.List;

/**
 * Created by xiaolu on 4/28/15.
 */
public class BlockBuilder {

    private final Options options;

    private ByteArrayDataOutput buffer = ByteStreams.newDataOutput();
    private Slice lastKey = new Slice();
    private boolean finished = false;
    private List<Integer> restarts = Lists.newArrayList();
    private int size = 0;
    private int counter = 0;

    public BlockBuilder(Options options) {
        Preconditions.checkNotNull(options);
        Preconditions.checkArgument(options.blockRestartInterval >= 1);

        this.options = options;
        restarts.add(0);
    }

    public void reset() {
        buffer = ByteStreams.newDataOutput();
        size = 0;
        restarts.clear();
        restarts.add(0);        // First restart point is at offset 0
        finished = false;
        lastKey = new Slice();
        counter = 0;
    }

    public boolean empty() {
        return size == 0;
    }

    public int currentSizeEstimate() {
        return size +                       // Raw data buffer
                restarts.size() * 4 +       // Restart array
                4;                           // Restart array length
    }

    public Slice finish() throws IOException {
        // Append restart array
        for (int restart : restarts) {
            Coding.putFixed32(buffer, restart);
            size += 4;
        }
        Coding.putFixed32(buffer, restarts.size());
        size += 4;
        finished = true;
        return new Slice(buffer.toByteArray());
    }

    public void add(Slice key, Slice value) throws IOException {
        Preconditions.checkState(!finished);
        Preconditions.checkState(counter <= options.blockRestartInterval);
        Preconditions.checkState(size == 0 // No values yet?
                || options.comparator.compare(key, lastKey) > 0);

        int shared = 0;
        if (counter < options.blockRestartInterval) {
            // See how much sharing to do with previous string
            int minLength = Math.min(lastKey.size(), key.size());
            while ((shared < minLength) && (lastKey.data()[shared] == key.data()[shared])) {
                shared++;
            }
        } else {
            // Restart compression
            restarts.add(size);
            counter = 0;
        }
        int nonShared = key.size() - shared;

        // Add "<shared><non_shared><value_size>" to buffer
        size += Coding.putVarint32(buffer, shared);
        size += Coding.putVarint32(buffer, nonShared);
        size += Coding.putVarint32(buffer, value.size());

        // Add string delta to buffer followed by value
        for (int i = 0; i < nonShared; i++) {
            buffer.writeByte(key.data()[i + shared]);
            size++;
        }
        buffer.write(value.data(), 0, value.size());
        size += value.size();

        // Update state
        lastKey = key;
        counter++;
    }
}
