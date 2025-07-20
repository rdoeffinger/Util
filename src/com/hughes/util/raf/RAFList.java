// Copyright 2011 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.hughes.util.raf;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

//import org.tukaani.xz.FinishableWrapperOutputStream;
//import org.tukaani.xz.LZMA2Options;
//import org.tukaani.xz.XZ;
//import org.tukaani.xz.XZOutputStream;

import com.hughes.util.ChunkedList;
import com.hughes.util.DataInputBuffer;
import com.hughes.util.StringUtil;

public class RAFList<T> extends AbstractList<T> implements RandomAccess, ChunkedList<T> {

    private static final int LONG_BYTES = Long.SIZE / 8;
    private static final int INT_BYTES = Integer.SIZE / 8;

    private final DataInputBuffer tocInput;
    private final DataInputBuffer dataInput;
    private final RAFListSerializer<T> serializer;
    private final RAFListSerializerSkippable<T> skippableSerializer;
    private final int size;
    private final long endOffset;
    private final int version;
    private final int blockSize;
    private final boolean compress;
    private final String debugstr;

    // buffer to avoid decompressing the same data over and over
    private int chunkDecBufIdx = -1;
    private DataInputBuffer chunkDecBuf;

    private RAFList(final DataInputBuffer in,
                    final RAFListSerializer<T> serializer,
                    int version, String debugstr)
    throws IOException {
        if (in.getStartFileOffset() < 0) {
            throw new RuntimeException("RAFList needs a value fileOffset!");
        }
        this.debugstr = debugstr;
            this.serializer = serializer;
            skippableSerializer = serializer instanceof RAFListSerializerSkippable ? (RAFListSerializerSkippable<T>)serializer : null;
            this.version = version;
            if (version >= 7) {
                size = StringUtil.readVarInt(in);
                blockSize = StringUtil.readVarInt(in);
                int flags = StringUtil.readVarInt(in);
                compress = (flags & 1) != 0;
            } else {
                size = in.readInt();
                blockSize = 1;
                compress = false;
            }
            int tocSize = ((size + blockSize - 1) / blockSize + 1) * (version >= 7 ? INT_BYTES : LONG_BYTES);
            tocInput = in.slice(tocSize);
            tocInput.position(tocSize - (version >= 7 ? INT_BYTES : LONG_BYTES));
            int dataSize = (version >= 7 ? tocInput.readInt() : (int)(tocInput.readLong() - tocInput.getStartFileOffset())) - tocSize;
            dataInput = in.slice(dataSize);
            endOffset = in.getFilePosition();
    }

    public long getEndOffset() {
        return endOffset;
    }

    @Override
    public int getMaxChunkSize() {
        return blockSize;
    }

    @Override
    public int getChunkStart(int index) {
        return (index / blockSize) * blockSize;
    }

    @Override
    public List<T> getChunk(int index) {
        return getChunk(index, 0, Math.min(blockSize, size - index));
    }

    private List<T> getChunk(int i, int skip, int len) {
        assert i == getChunkStart(i);
        assert len <= blockSize;
        List<T> res = new ArrayList<>(len);
        try {
                if (chunkDecBufIdx == i) {
                    chunkDecBuf.rewind();
                } else {
                    int tocofs = (i / blockSize) * (version >= 7 ? INT_BYTES : LONG_BYTES);
                    tocInput.position(tocofs);
                    final long start = version >= 7 ? tocInput.readInt() + tocInput.getStartFileOffset() : tocInput.readLong();
                    final long end = version >= 7 ? tocInput.readInt() + tocInput.getStartFileOffset() : tocInput.readLong();
                    dataInput.position((int)(start - dataInput.getStartFileOffset()));
                    if (compress) {
                        // In theory using the InflaterInputStream directly should be
                        // possible, decompressing all at once should be better.
                        // We need the byte array as a hack to be able to signal EOF
                        // to the Inflater at the right point in time.
                        chunkDecBufIdx = -1;
                        chunkDecBuf = null;
                        byte[] inBytes = new byte[Math.min((int)(end - start), 20 * 1024 * 1024)];
                        dataInput.readFully(inBytes);
                        inBytes = StringUtil.unzipFully(inBytes, -1);
                        chunkDecBuf = new DataInputBuffer(ByteBuffer.wrap(inBytes));
                        chunkDecBufIdx = i;
                    }
                }
                DataInput in = compress ? chunkDecBuf : dataInput;
                for (int cur = i; cur < i + skip; ++cur) {
                    assert skippableSerializer != null;
                    if (skippableSerializer != null) skippableSerializer.skip(in, cur);
                    else serializer.read(in, cur);
                }
                for (int cur = i + skip; cur < i + len; ++cur) {
                    res.add(serializer.read(in, cur));
                }
                return res;
        } catch (IOException e) {
            throw new RuntimeException(debugstr + "Failed reading dictionary entries " + i + " - " + (i + len - 1) + ", possible data corruption?", e);
        }
    }

    @Override
    public T get(final int i) {
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException(i + ", size=" + size);
        }
        int start = getChunkStart(i);
        List<T> chunk = getChunk(start, i - start, i - start + 1);
        return chunk.get(0);
    }

    @Override
    public int size() {
        return size;
    }

    public static <T> RAFList<T> create(final DataInputBuffer in,
                                        final RAFListSerializer<T> serializer,
                                        int version, String debugstr)
    throws IOException {
        return new RAFList<>(in, serializer, version, debugstr);
    }

    /**
     * Same, but deserialization ignores indices.
     */
    public static <T> RAFList<T> create(final DataInputBuffer in,
                                        final RAFSerializer<T> serializer,
                                        int version, String debugstr)
            throws IOException {
        return new RAFList<>(in, getWrapper(serializer), version, debugstr);
    }

    private static class BlockCompressor<T> implements Runnable {
        public BlockCompressor(final ConcurrentLinkedQueue<Deflater> deflaterCache,
                               final List<T> list,
                               final RAFListSerializer<T> serializer,
                               int blockStart, int blockEnd) {
            this.deflaterCache = deflaterCache;
            this.list = list;
            this.serializer = serializer;
            this.blockStart = blockStart;
            this.blockEnd = blockEnd;
            sem = new Semaphore(0);
            error = null;
        }

        private final ConcurrentLinkedQueue<Deflater> deflaterCache;
        private final List<T> list;
        private final RAFListSerializer<T> serializer;
        private final int blockStart;
        private final int blockEnd;
        public Semaphore sem;
        public byte[] data;
        public int uncompSize;
        public Exception error;

        @Override
        public void run() {
            try {
                Deflater d = deflaterCache.poll();
                if (d == null) d = new Deflater(9);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                //OutputStream stream = new LZMA2Options().getOutputStream(new FinishableWrapperOutputStream(baos));
                OutputStream stream = new DeflaterOutputStream(baos, d);
                DataOutputStream outstream = new DataOutputStream(new BufferedOutputStream(stream));
                for (int i = blockStart; i < blockEnd; i++) {
                    serializer.write(outstream, list.get(i));
                }
                outstream.close();
                data = baos.toByteArray();
                uncompSize = outstream.size();
                d.reset();
                deflaterCache.add(d);
            } catch (Exception e) {
                error = e;
            }
            sem.release();
        }
    }

    public static <T> void write(final RandomAccessFile raf,
                                 final List<T> list, final RAFListSerializer<T> serializer,
                                 int block_size, boolean compress)
    throws IOException {
        StringUtil.writeVarInt(raf, list.size());
        StringUtil.writeVarInt(raf, block_size);
        StringUtil.writeVarInt(raf, compress ? 1 : 0);
        long tocStart = raf.getFilePointer();
        int blockCnt = (list.size() + block_size - 1) / block_size;
        int tocSize = INT_BYTES * (blockCnt + 1);
        ByteBuffer tocData = ByteBuffer.allocate(tocSize);
        raf.seek(tocStart + tocSize);
        final long dataStart = raf.getFilePointer();

        final ArrayList<BlockCompressor<T>> blocks = new ArrayList<>(blockCnt);
        if (compress) {
            ConcurrentLinkedQueue<Deflater> deflaterCache = new ConcurrentLinkedQueue<>();
            try (ExecutorService e = Executors.newCachedThreadPool()) {
                for (int i = 0; i < blockCnt; i++) {
                    int start = i * block_size;
                    int end = Math.min(start + block_size, list.size());
                    BlockCompressor<T> bb = new BlockCompressor<>(deflaterCache, list, serializer, start, end);
                    e.execute(bb);
                    blocks.add(bb);
                }
            }
        }

        int maxBlock = 0;
        int minBlock = 0x7fffffff;
        int sumBlock = 0;
        int maxBlockC = 0;
        int minBlockC = 0x7fffffff;
        int sumBlockC = 0;
        int numBlock = 0;
        for (int i = 0; i < blockCnt; i++) {
            long startOffset = raf.getFilePointer();
            tocData.putInt((int)(startOffset - tocStart));
            if (compress) {
                BlockCompressor<T> b = blocks.get(i);
                try {
                    b.sem.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (b.error != null) throw new RuntimeException(b.error);
                maxBlock = Math.max(maxBlock, b.uncompSize);
                minBlock = Math.min(minBlock, b.uncompSize);
                sumBlock += b.uncompSize;
                maxBlockC = Math.max(maxBlockC, b.data.length);
                minBlockC = Math.min(minBlockC, b.data.length);
                sumBlockC += b.data.length;
                numBlock++;
                raf.write(b.data);
            } else {
                int start = i * block_size;
                int end = Math.min(start + block_size, list.size());
                for (int j = start; j < end; j++) {
                    serializer.write(raf, list.get(j));
                }
            }
        }
        System.out.println("RAFList stats: " + numBlock + "x" + block_size + " entries");
        if (numBlock > 0) {
            System.out.println("uncompressed min " + minBlock + ", max " + maxBlock + ", sum " + sumBlock + ", average " + sumBlock / (float)numBlock);
            System.out.println("compressed min " + minBlockC + ", max " + maxBlockC + ", sum " + sumBlockC + ", average " + sumBlockC / (float)numBlock);
        }
        final long endOffset = raf.getFilePointer();
        tocData.putInt((int)(endOffset - tocStart));
        raf.seek(tocStart);
        raf.write(tocData.array());
        assert dataStart == raf.getFilePointer();
        raf.seek(endOffset);
    }

    public static <T> void write(final RandomAccessFile raf,
                                 final List<T> list, final RAFListSerializer<T> serializer)
    throws IOException {
        write(raf, list, serializer, 1, false);
    }
    public static <T> void write(final RandomAccessFile raf,
                                 final List<T> list, final RAFSerializer<T> serializer,
                                 int block_size, boolean compress)
    throws IOException {
        write(raf, list, getWrapper(serializer), block_size, compress);
    }

    public static <T> void write(final RandomAccessFile raf,
                                 final List<T> list, final RAFSerializer<T> serializer)
    throws IOException {
        write(raf, list, getWrapper(serializer), 1, false);
    }

    public static <T> RAFListSerializer<T> getWrapper(final RAFSerializer<T> serializer) {
        return new RAFListSerializer.Wrapper<>(serializer);
    }


}
