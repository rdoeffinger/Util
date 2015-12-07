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

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.util.AbstractList;
import java.util.Collection;
import java.util.RandomAccess;
import java.util.zip.DeflaterOutputStream;

import com.hughes.util.StringUtil;

public class RAFList<T> extends AbstractList<T> implements RandomAccess {

  private static final int LONG_BYTES = Long.SIZE / 8;
  private static final int INT_BYTES = Integer.SIZE / 8;

  final RandomAccessFile raf;
  final RAFListSerializer<T> serializer;
  final long tocOffset;
  final int size;
  final long endOffset;
  final int version;
  final int blockSize;
  final boolean compress;

  public RAFList(final RandomAccessFile raf,
      final RAFListSerializer<T> serializer, final long startOffset,
      int version, int blockSize, boolean compress)
      throws IOException {
    synchronized (raf) {
      this.raf = raf;
      this.serializer = serializer;
      this.version = version;
      this.blockSize = blockSize;
      this.compress = compress;
      raf.seek(startOffset);
      size = raf.readInt();
      this.tocOffset = raf.getFilePointer();

      raf.seek(tocOffset + (size + blockSize - 1) / blockSize * (version >= 7 ? INT_BYTES : LONG_BYTES));
      endOffset = version >= 7 ? tocOffset + raf.readInt() : raf.readLong();
      raf.seek(endOffset);
    }
  }

  public long getEndOffset() {
    return endOffset;
  }

  @Override
  public T get(final int i) {
    if (i < 0 || i >= size) {
      throw new IndexOutOfBoundsException(i + ", size=" + size);
    }
    try {
      synchronized (raf) {
        raf.seek(tocOffset + (i / blockSize) * (version >= 7 ? INT_BYTES : LONG_BYTES));
        final long start = version >= 7 ? tocOffset + raf.readInt() : raf.readLong();
        final long end = version >= 7 ? tocOffset + raf.readInt() : raf.readLong();
        raf.seek(start);
        T res = null;
        DataInput in = raf;
        if (compress) {
            // In theory using the InflaterInputStream directly should be
            // possible, decompressing all at once should be better.
            // We need the byte array as a hack to be able to signal EOF
            // to the Inflater at the right point in time.
            byte[] inBytes = new byte[(int)(end - start)];
            raf.read(inBytes);
            inBytes = StringUtil.unzipFully(inBytes, -1);
            in = new DataInputStream(new ByteArrayInputStream(inBytes));
        }
        for (int cur = (i / blockSize) * blockSize; cur <= i; ++cur) {
            res = serializer.read(in, cur);
        }
        return res;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int size() {
    return size;
  }

  public static <T> RAFList<T> create(final RandomAccessFile raf,
      final RAFListSerializer<T> serializer, final long startOffset,
      int version)
      throws IOException {
    return new RAFList<T>(raf, serializer, startOffset, version, 1, false);
  }

  public static <T> RAFList<T> create(final RandomAccessFile raf,
      final RAFListSerializer<T> serializer, final long startOffset,
      int version, int blockSize, boolean compress)
      throws IOException {
    return new RAFList<T>(raf, serializer, startOffset, version, blockSize, compress);
  }

  /**
   * Same, but deserialization ignores indices.
   */
  public static <T> RAFList<T> create(final RandomAccessFile raf,
      final RAFSerializer<T> serializer, final long startOffset,
      int version)
      throws IOException {
    return new RAFList<T>(raf, getWrapper(serializer), startOffset, version, 1, false);
  }

  public static <T> RAFList<T> create(final RandomAccessFile raf,
      final RAFSerializer<T> serializer, final long startOffset,
      int version, int blockSize, boolean compress)
      throws IOException {
    return new RAFList<T>(raf, getWrapper(serializer), startOffset, version, blockSize, compress);
  }

  public static <T> void write(final RandomAccessFile raf,
      final Collection<T> list, final RAFListSerializer<T> serializer,
      int block_size, boolean compress)
      throws IOException {
    raf.writeInt(list.size());
    long tocPos = raf.getFilePointer();
    long tocStart = tocPos;
    raf.seek(tocPos + INT_BYTES * ((list.size() + block_size - 1) / block_size + 1));
    int i = 0;
    final long dataStart = raf.getFilePointer();
    long startOffset = raf.getFilePointer();
    DataOutputStream compress_out = null;
    ByteArrayOutputStream outstream = null;
    for (final T t : list) {
      if ((i % block_size) == 0) {
          if (compress_out != null) {
              compress_out.close();
              compress_out = null;
              raf.write(outstream.toByteArray());
              outstream = null;
          }
          startOffset = raf.getFilePointer();
          raf.seek(tocPos);
          raf.writeInt((int)(startOffset - tocStart));
          tocPos = raf.getFilePointer();
          raf.seek(startOffset);
          if (compress) {
              outstream = new ByteArrayOutputStream();
              compress_out = new DataOutputStream(new DeflaterOutputStream(outstream));
          }
      }
      serializer.write(compress ? compress_out : raf, t);
      ++i;
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
    return new RAFListSerializer.Wrapper<T>(serializer);
  }


}
