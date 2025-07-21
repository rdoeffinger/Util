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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;

import com.hughes.util.ChunkedList;
import com.hughes.util.DataInputBuffer;

public class UniformRAFList<T> extends AbstractList<T> implements RandomAccess, ChunkedList<T> {

    private static final int blockSize = 32;
    private final DataInputBuffer dataInput;
    private final RAFListSerializer<T> serializer;
    private final int size;
    private final int datumSize;
    private final long endOffset;

    private UniformRAFList(final DataInputBuffer in,
                           final RAFListSerializer<T> serializer) {
        this.serializer = serializer;
        size = in.readInt();
        datumSize = in.readInt();
        int dataSize = size * datumSize;
        endOffset = in.getFilePosition() >= 0 ? in.getFilePosition() + dataSize : -1;
        dataInput = in.slice(dataSize);
    }

    public long getEndOffset() {
        if (endOffset < 0) {
            throw new RuntimeException("getEndOffset called for input buffer with unknown file position");
        }
        return endOffset;
    }

    @Override
    public T get(final int i) {
        if (i < 0 || i >= size) {
            throw new IndexOutOfBoundsException("" + i);
        }
        dataInput.position(i * datumSize);
        final T result;
        try {
            result = serializer.read(dataInput, i);
        } catch (IOException e) {
            throw new RuntimeException("Failed reading entry, dictionary corrupted?", e);
        }
        if (dataInput.position() != (i + 1) * datumSize) {
            throw new RuntimeException("Read "
                                       + (dataInput.position() - i * datumSize)
                                       + " bytes, should have read " + datumSize);
        }
        return result;
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
        dataInput.position(i * datumSize);
        try {
            for (int cur = i; cur < i + len; ++cur) {
                res.add(serializer.read(dataInput, cur));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed reading entry, dictionary corrupted?", e);
        }
        if (dataInput.position() != (i + len) * datumSize) {
            throw new RuntimeException("Read "
                                      + (dataInput.position() - i * datumSize)
                                      + " bytes, should have read " + len * datumSize);
        }
        return res;
    }

    public static <T> UniformRAFList<T> create(final DataInputBuffer in,
            final RAFListSerializer<T> serializer) {
        return new UniformRAFList<>(in, serializer);
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
}
