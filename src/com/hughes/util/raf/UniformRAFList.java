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

import com.hughes.util.ChunkedList;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
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

public class UniformRAFList<T> extends AbstractList<T> implements RandomAccess, ChunkedList<T> {

    private static final int blockSize = 32;
    private final FileChannel ch;
    private final DataInput raf;
    private final RAFListSerializer<T> serializer;
    private final int size;
    private final int datumSize;
    private final long dataStart;
    private final long endOffset;

    private UniformRAFList(final FileChannel ch,
                           final RAFListSerializer<T> serializer, final long startOffset)
    throws IOException {
        synchronized (ch) {
            this.ch = ch;
            this.raf = new DataInputStream(Channels.newInputStream(this.ch));
            this.serializer = serializer;
            ch.position(startOffset);

            size = raf.readInt();
            datumSize = raf.readInt();
            dataStart = ch.position();
            endOffset = dataStart + size * datumSize;
            ch.position(endOffset);
        }
    }

    public long getEndOffset() {
        return endOffset;
    }

    @Override
    public T get(final int i) {
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException("" + i);
        }
        try {
            synchronized (ch) {
                ch.position(dataStart + i * datumSize);
                final T result = serializer.read(raf, i);
                if (ch.position() != dataStart + (i + 1) * datumSize) {
                    throw new RuntimeException("Read "
                                               + (ch.position() - (dataStart + i * datumSize))
                                               + " bytes, should have read " + datumSize);
                }
                return result;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed reading entry, dictionary corrupted?", e);
        }
    }

    @Override
    public int size() {
        return size;
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
    public List<T> getChunk(int i) {
        int len = Math.min(blockSize, size - i);
        List<T> res = new ArrayList<>(len);
        try {
            synchronized (ch) {
                ch.position(dataStart + i * datumSize);
                for (int cur = i; cur < i + len; ++cur) {
                    res.add(serializer.read(raf, cur));
                }
                if (ch.position() != dataStart + (i + len) * datumSize) {
                    throw new RuntimeException("Read "
                                               + (ch.position() - (dataStart + i * datumSize))
                                               + " bytes, should have read " + len * datumSize);
                }
                return res;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed reading entry, dictionary corrupted?", e);
        }
    }

    public static <T> UniformRAFList<T> create(final FileChannel raf,
            final RAFListSerializer<T> serializer, final long startOffset)
    throws IOException {
        return new UniformRAFList<>(raf, serializer, startOffset);
    }
    public static <T> UniformRAFList<T> create(final FileChannel raf,
            final RAFSerializer<T> serializer, final long startOffset)
    throws IOException {
        return new UniformRAFList<>(raf, RAFList.getWrapper(serializer), startOffset);
    }

    private static long getOffset(DataOutput out) throws IOException {
        if (out instanceof RandomAccessFile)
            return ((RandomAccessFile)out).getFilePointer();
        if (out instanceof DataOutputStream)
            return ((DataOutputStream)out).size();
        return -1;
    }

    public static <T> void write(final DataOutput raf,
                                 final Collection<T> list, final RAFListSerializer<T> serializer,
                                 final int datumSize) throws IOException {
        raf.writeInt(list.size());
        raf.writeInt(datumSize);
        for (final T t : list) {
            final long startOffset = getOffset(raf);
            serializer.write(raf, t);
            if (startOffset != -1 && getOffset(raf) != startOffset + datumSize) {
                throw new RuntimeException("Wrote "
                                           + (getOffset(raf) - startOffset)
                                           + " bytes, should have written " + datumSize);
            }
        }
    }

    public static <T> void write(final RandomAccessFile raf,
                                 final Collection<T> list, final RAFSerializer<T> serializer,
                                 final int datumSize) throws IOException {
        write(raf, list, new RAFListSerializer<T>() {
            @Override
            public T read(DataInput raf, final int readIndex)
            throws IOException {
                return serializer.read(raf);
            }
            @Override
            public void write(DataOutput raf, T t) throws IOException {
                serializer.write(raf, t);
            }
        }, datumSize);
    }
}
