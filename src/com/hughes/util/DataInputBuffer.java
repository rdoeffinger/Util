// Copyright 2020 Reimar Döffinger. All Rights Reserved.
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

package com.hughes.util;

import java.io.DataInput;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public final class DataInputBuffer implements DataInput {
    public final ByteBuffer b;
    public final long fileOffset;

    public DataInputBuffer(ByteBuffer buf) {
        b = buf;
        fileOffset = -1;
    }

    public DataInputBuffer(ByteBuffer buf, long fileOffset) {
        b = buf;
        this.fileOffset = fileOffset;
    }

    public long getStartFileOffset() {
        return fileOffset;
    }

    public long getFilePosition() {
        return fileOffset + b.position();
    }

    public int limit() {
        return b.limit();
    }

    public DataInputBuffer slice(int size) {
        DataInputBuffer res = new DataInputBuffer(b.slice(), getFilePosition());
        res.b.limit(size);
        skipBytes(size);
        return res;
    }

    public String readUTF() {
        int len = readUnsignedShort();
        char[] str = new char[len];
        if (len == 0) return "";
        int opos = 0;
        int c = b.get();
        if (c < 0) c &= 0x1f;
        for (int i = 1; i < len; i++) {
            int n = b.get();
            if (n < -64) {
                c <<= 6;
                c |= n & 0x3f;
            } else {
                str[opos++] = (char)c;
                c = n;
                if (c < 0) c &= 0x1f;
            }
        }
        str[opos++] = (char)c;
        return new String(str, 0, opos);
    }

    public String readLine() {
        throw new RuntimeException("readLine() not implemented in DataInputBuffer!");
    }

    public double readDouble() { return b.getDouble(); }
    public float readFloat() { return b.getFloat(); }
    public long readLong() { return b.getLong(); }
    public int readInt() { return b.getInt(); }
    public char readChar() { return b.getChar(); }
    public int readUnsignedShort() { return b.getShort() & 0xffff; }
    public short readShort() { return b.getShort(); }
    public int readUnsignedByte() { return b.get() & 0xff; }
    public byte readByte() { return b.get(); }
    public boolean readBoolean() { return b.get() != 0; }
    public int skipBytes(int n) {
        n = Math.max(0, n);
        n = Math.min(n, b.remaining());
        b.position(b.position() + n);
        return n;
    }
    public void readFully(byte[] buf, int off, int len) { b.get(buf, off, len); }
    public void readFully(byte[] buf) { b.get(buf); }

    public String asString() {
        try {
            return new String(b.array(), b.arrayOffset(), b.limit(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Missing UTF-8 support?!", e);
        }
    }

    public DataInputBuffer rewind() {
        b.rewind();
        return this;
    }
    public int position() {
        return b.position();
    }
    public void position(int p) {
        b.position(p);
    }
}
