package com.github.zxl0714.leveldb;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.iq80.snappy.Snappy;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Created by xiaolu on 4/28/15.
 */
public class TableBuilder {

    private final Options options;
    private final Options indexBlockOptions;
    private final DataOutputStream out;
    private final BlockBuilder dataBlock;
    private final BlockBuilder indexBlock;
    private FilterBlockBuilder filterBlock = null;

    private long offset = 0;
    private long numEntries = 0;
    private boolean closed = false;
    private Slice lastKey;
    private boolean pendingIndexEntry = false;
    private TableFormat.BlockHandle pendingHandle = new TableFormat.BlockHandle();
    private byte[] compressBuffer = new byte[8192];

    public TableBuilder(Options options, OutputStream out) throws IOException {
        this.options = new Options(options);
        this.out = new DataOutputStream(out);
        this.indexBlockOptions = new Options(options);
        this.indexBlockOptions.blockRestartInterval = 1;
        this.dataBlock = new BlockBuilder(this.options);
        this.indexBlock = new BlockBuilder(this.indexBlockOptions);
        if (options.filterPolicy != null) {
            filterBlock = new FilterBlockBuilder(options.filterPolicy);
            filterBlock.startBlock(0);
        }
    }

    public void add(Slice key, Slice value) throws IOException {
        Preconditions.checkState(!closed);

        if (numEntries > 0) {
            Preconditions.checkState(options.comparator.compare(key, lastKey) > 0);
        }

        if (pendingIndexEntry) {
            Preconditions.checkState(dataBlock.empty());
            lastKey = options.comparator.findShortestSeparator(lastKey, key);
            ByteArrayDataOutput handleEncoding = ByteStreams.newDataOutput();
            pendingHandle.encodeTo(handleEncoding);
            indexBlock.add(lastKey, new Slice(handleEncoding.toByteArray()));
            pendingIndexEntry = false;
        }

        if (filterBlock != null) {
            filterBlock.addKey(key);
        }

        lastKey = key;
        numEntries++;
        dataBlock.add(key, value);

        int estimatedBlockSize = dataBlock.currentSizeEstimate();
        if (estimatedBlockSize >= options.blockSize) {
            flush();
        }
    }

    private void flush() throws IOException {
        Preconditions.checkState(!closed);

        if (dataBlock.empty()) {
            return ;
        }
        Preconditions.checkState(!pendingIndexEntry);
        writeBlock(dataBlock, pendingHandle);
        pendingIndexEntry = true;
        if (filterBlock != null) {
            filterBlock.startBlock((int) offset);
        }
    }

    private void writeBlock(BlockBuilder block, TableFormat.BlockHandle handle) throws IOException {
        Slice raw = block.finish();
        Slice blockContents;
        int type = options.compression;
        switch (type) {
            case Options.CompressionType.NO_COMPRESSION:
                blockContents = raw;
                break;

            case Options.CompressionType.SNAPPY_COMPRESSION:
                int maxLength = Snappy.maxCompressedLength(raw.data().length);
                if (maxLength > compressBuffer.length) {
                    compressBuffer = new byte[maxLength * 2];
                }
                int compressLength = Snappy.compress(raw.data(), 0, raw.size(), compressBuffer, 0);
                if (compressLength < raw.size() - raw.size() / 8) {
                    blockContents = new Slice(Arrays.copyOf(compressBuffer, compressLength));
                } else {
                    blockContents = raw;
                    type = Options.CompressionType.NO_COMPRESSION;
                }
                break;
            default:
                throw new IOException("Compression type not supported");
        }
        writeRawBlock(blockContents, type, handle);
        block.reset();
    }

    private void writeRawBlock(Slice blockContents, int type, TableFormat.BlockHandle handle) throws IOException {
        handle.setOffset(offset);
        handle.setSize(blockContents.size());
        out.write(blockContents.data());
        byte[] trailer = new byte[TableFormat.BLOCK_TRAILER_SIZE];
        trailer[0] = (byte) type;
        int crc = Crc32c.value(blockContents.data(), blockContents.size());
        crc = Crc32c.extend(crc, trailer, 0, 1);
        byte[] crcEncode = Coding.encodeFixed32(Crc32c.mask(crc));
        System.arraycopy(crcEncode, 0, trailer, 1, 4);
        out.write(trailer);
        offset += blockContents.size() + TableFormat.BLOCK_TRAILER_SIZE;
    }

    public void finish() throws IOException {
        flush();
        Preconditions.checkState(!closed);
        closed = true;

        TableFormat.BlockHandle filterBlockHandle = new TableFormat.BlockHandle();
        TableFormat.BlockHandle metaindexBlockHandle = new TableFormat.BlockHandle();
        TableFormat.BlockHandle indexBlockHandle = new TableFormat.BlockHandle();

        // Write filter block
        if (filterBlock != null) {
            writeRawBlock(filterBlock.finish(), Options.CompressionType.NO_COMPRESSION, filterBlockHandle);
        }

        // Write metaindex block
        {
            BlockBuilder metaIndexBlock = new BlockBuilder(options);
            if (filterBlock != null) {
                ByteArrayDataOutput handleEncode = ByteStreams.newDataOutput();
                filterBlockHandle.encodeTo(handleEncode);
                metaIndexBlock.add(new Slice("filter." + options.filterPolicy.name()), new Slice(handleEncode.toByteArray()));
            }
            writeBlock(metaIndexBlock, metaindexBlockHandle);
        }

        // Write index block
        {
            if (pendingIndexEntry) {
                lastKey = options.comparator.findShortSuccessor(lastKey);
                ByteArrayDataOutput handleEncode = ByteStreams.newDataOutput();
                pendingHandle.encodeTo(handleEncode);
                indexBlock.add(lastKey, new Slice(handleEncode.toByteArray()));
                pendingIndexEntry = false;
            }
            writeBlock(indexBlock, indexBlockHandle);
        }

        // Write footer
        {
            TableFormat.Footer footer = new TableFormat.Footer();
            footer.setMetaindexHandle(metaindexBlockHandle);
            footer.setIndexHandle(indexBlockHandle);
            footer.encodeTo(out);
            offset += TableFormat.Footer.ENCODED_LENGTH;
        }
    }

    public long fileSize() {
        return offset;
    }
}
