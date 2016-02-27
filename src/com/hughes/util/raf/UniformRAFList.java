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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractList;
import java.util.Collection;
import java.util.RandomAccess;

public class UniformRAFList<T> extends AbstractList<T> implements RandomAccess {

  final RandomAccessFile raf;
  final RAFListSerializer<T> serializer;
  final int size;
  final int datumSize;
  final long dataStart;
  final long endOffset;

  public UniformRAFList(final RandomAccessFile raf,
      final RAFListSerializer<T> serializer, final long startOffset)
      throws IOException {
    synchronized (raf) {
      this.raf = raf;
      this.serializer = serializer;
      raf.seek(startOffset);

      size = raf.readInt();
      datumSize = raf.readInt();
      dataStart = raf.getFilePointer();
      endOffset = dataStart + size * datumSize;
      raf.seek(endOffset);
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
      synchronized (raf) {
        raf.seek(dataStart + i * datumSize);
        final T result = serializer.read(raf, i);
        if (raf.getFilePointer() != dataStart + (i + 1) * datumSize) {
          throw new RuntimeException("Read "
              + (raf.getFilePointer() - (dataStart + i * datumSize))
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

  public static <T> UniformRAFList<T> create(final RandomAccessFile raf,
      final RAFListSerializer<T> serializer, final long startOffset)
      throws IOException {
    return new UniformRAFList<T>(raf, serializer, startOffset);
  }
  public static <T> UniformRAFList<T> create(final RandomAccessFile raf,
      final RAFSerializer<T> serializer, final long startOffset)
      throws IOException {
    return new UniformRAFList<T>(raf, RAFList.getWrapper(serializer), startOffset);
  }

  static long getOffset(DataOutput out) throws IOException {
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
      }}, datumSize);
  }
}
