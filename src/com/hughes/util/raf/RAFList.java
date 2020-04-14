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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

//import org.tukaani.xz.FinishableWrapperOutputStream;
//import org.tukaani.xz.LZMA2Options;
//import org.tukaani.xz.XZ;
//import org.tukaani.xz.XZOutputStream;

import com.hughes.util.ChunkedList;
import com.hughes.util.StringUtil;

public class RAFList<T> extends AbstractList<T> implements RandomAccess, ChunkedList<T> {

    private static final int LONG_BYTES = Long.SIZE / 8;
    private static final int INT_BYTES = Integer.SIZE / 8;

    private final FileChannel ch;
    private final DataInput raf;
    private final RAFListSerializer<T> serializer;
    private final RAFListSerializerSkippable<T> skippableSerializer;
    private final long tocOffset;
    private final int size;
    private final long endOffset;
    private final int version;
    private final int blockSize;
    private final boolean compress;
    private final String debugstr;

    // buffer to avoid decompressing the same data over and over
    private int chunkDecBufIdx = -1;
    private byte[] chunkDecBuf;

    private RAFList(final FileChannel ch,
                    final RAFListSerializer<T> serializer, final long startOffset,
                    int version, String debugstr)
    throws IOException {
        this.debugstr = debugstr;
        synchronized (ch) {
            this.ch = ch;
            this.raf = new DataInputStream(Channels.newInputStream(this.ch));
            this.serializer = serializer;
            skippableSerializer = serializer instanceof RAFListSerializerSkippable ? (RAFListSerializerSkippable<T>)serializer : null;
            this.version = version;
            ch.position(startOffset);
            if (version >= 7) {
                size = StringUtil.readVarInt(raf);
                blockSize = StringUtil.readVarInt(raf);
                int flags = StringUtil.readVarInt(raf);
                compress = (flags & 1) != 0;
            } else {
                size = raf.readInt();
                blockSize = 1;
                compress = false;
            }
            this.tocOffset = ch.position();

            ch.position(tocOffset + (size + blockSize - 1) / blockSize * (version >= 7 ? INT_BYTES : LONG_BYTES));
            endOffset = version >= 7 ? tocOffset + raf.readInt() : raf.readLong();
            ch.position(endOffset);
        }
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
            synchronized (ch) {
                if (chunkDecBufIdx != i) {
                    ch.position(tocOffset + (i / blockSize) * (version >= 7 ? INT_BYTES : LONG_BYTES));
                    final long start = version >= 7 ? tocOffset + raf.readInt() : raf.readLong();
                    final long end = version >= 7 ? tocOffset + raf.readInt() : raf.readLong();
                    ch.position(start);
                    if (compress) {
                        // In theory using the InflaterInputStream directly should be
                        // possible, decompressing all at once should be better.
                        // We need the byte array as a hack to be able to signal EOF
                        // to the Inflater at the right point in time.
                        chunkDecBuf = null;
                        byte[] inBytes = new byte[Math.min((int)(end - start), 20 * 1024 * 1024)];
                        raf.readFully(inBytes);
                        inBytes = StringUtil.unzipFully(inBytes, -1);
                        chunkDecBufIdx = i;
                        chunkDecBuf = inBytes;
                    }
                }
                DataInput in = compress ? new DataInputStream(new ByteArrayInputStream(chunkDecBuf)) : raf;
                for (int cur = i; cur < i + skip; ++cur) {
                    assert skippableSerializer != null;
                    if (skippableSerializer != null) skippableSerializer.skip(in, cur);
                    else serializer.read(in, cur);
                }
                for (int cur = i + skip; cur < i + len; ++cur) {
                    res.add(serializer.read(in, cur));
                }
                return res;
            }
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

    public static <T> RAFList<T> create(final FileChannel raf,
                                        final RAFListSerializer<T> serializer, final long startOffset,
                                        int version, String debugstr)
    throws IOException {
        return new RAFList<>(raf, serializer, startOffset, version, debugstr);
    }

    /**
     * Same, but deserialization ignores indices.
     */
    public static <T> RAFList<T> create(final FileChannel raf,
                                        final RAFSerializer<T> serializer, final long startOffset,
                                        int version, String debugstr)
    throws IOException {
        return new RAFList<>(raf, getWrapper(serializer), startOffset, version, debugstr);
    }

    public static <T> void write(final RandomAccessFile raf,
                                 final Collection<T> list, final RAFListSerializer<T> serializer,
                                 int block_size, boolean compress)
    throws IOException {
        StringUtil.writeVarInt(raf, list.size());
        StringUtil.writeVarInt(raf, block_size);
        StringUtil.writeVarInt(raf, compress ? 1 : 0);
        long tocPos = raf.getFilePointer();
        long tocStart = tocPos;
        raf.seek(tocPos + INT_BYTES * ((list.size() + block_size - 1) / block_size + 1));
        int i = 0;
        final long dataStart = raf.getFilePointer();
        DataOutputStream compress_out = null;
        ByteArrayOutputStream outstream = new ByteArrayOutputStream();
        int maxBlock = 0;
        int minBlock = 0x7fffffff;
        int sumBlock = 0;
        int maxBlockC = 0;
        int minBlockC = 0x7fffffff;
        int sumBlockC = 0;
        int numBlock = 0;
        for (final T t : list) {
            if ((i % block_size) == 0) {
                if (compress_out != null) {
                    compress_out.close();
                    maxBlock = Math.max(maxBlock, compress_out.size());
                    minBlock = Math.min(minBlock, compress_out.size());
                    sumBlock += compress_out.size();
                    maxBlockC = Math.max(maxBlockC, outstream.size());
                    minBlockC = Math.min(minBlockC, outstream.size());
                    sumBlockC += outstream.size();
                    numBlock++;
                    compress_out = null;
                    raf.write(outstream.toByteArray());
                    outstream.reset();
                    if ((i % 32768) == 0) {
                        // Otherwise at least OpenJDK 8 falls completely over.
                        System.gc();
                    }
                }
                long startOffset = raf.getFilePointer();
                raf.seek(tocPos);
                raf.writeInt((int)(startOffset - tocStart));
                tocPos = raf.getFilePointer();
                raf.seek(startOffset);
                if (compress) {
                    //compress_out = new DataOutputStream(new LZMA2Options().getOutputStream(new FinishableWrapperOutputStream(outstream)));
                    compress_out = new DataOutputStream(new DeflaterOutputStream(outstream, new Deflater(9)));
                }
            }
            serializer.write(compress ? compress_out : raf, t);
            ++i;
        }
        System.out.println("RAFList stats: " + numBlock + "x" + block_size + " entries");
        if (numBlock > 0) {
            System.out.println("uncompressed min " + minBlock + ", max " + maxBlock + ", sum " + sumBlock + ", average " + sumBlock / (float)numBlock);
            System.out.println("compressed min " + minBlockC + ", max " + maxBlockC + ", sum " + sumBlockC + ", average " + sumBlockC / (float)numBlock);
        }
        if (compress_out != null) {
            compress_out.close();
            compress_out = null;
            raf.write(outstream.toByteArray());
            outstream = null;
        }
        final long endOffset = raf.getFilePointer();
        raf.seek(tocPos);
        raf.writeInt((int)(endOffset - tocStart));
        assert dataStart == raf.getFilePointer();
        raf.seek(endOffset);
    }

    public static <T> void write(final RandomAccessFile raf,
                                 final Collection<T> list, final RAFListSerializer<T> serializer)
    throws IOException {
        write(raf, list, serializer, 1, false);
    }
    public static <T> void write(final RandomAccessFile raf,
                                 final Collection<T> list, final RAFSerializer<T> serializer,
                                 int block_size, boolean compress)
    throws IOException {
        write(raf, list, getWrapper(serializer), block_size, compress);
    }

    public static <T> void write(final RandomAccessFile raf,
                                 final Collection<T> list, final RAFSerializer<T> serializer)
    throws IOException {
        write(raf, list, getWrapper(serializer), 1, false);
    }

    public static <T> RAFListSerializer<T> getWrapper(final RAFSerializer<T> serializer) {
        return new RAFListSerializer.Wrapper<>(serializer);
    }


}
