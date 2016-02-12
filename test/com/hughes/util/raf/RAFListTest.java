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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


import junit.framework.TestCase;

public class RAFListTest extends TestCase {

  public void testFileListString() throws IOException {
    testFileList(RAFSerializer.STRING);
  }

  public void testFileListSerializable() throws IOException {
    testFileList(new SerializableSerializer<String>());
  }

  public void testFileList(final RAFSerializer<String> serializer) throws IOException {
    final File file = File.createTempFile("asdf", "asdf");
    file.deleteOnExit();
    try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {

    raf.writeUTF("Hello World!");
    final List<String> l1 = Arrays.asList("1a", "1bc", "1def");
    final List<String> l2 = Arrays.asList("2aa", "2abc", "2adef");
    RAFList.write(raf, l1, serializer);
    RAFList.write(raf, l2, serializer);
    raf.writeUTF("Goodbye World!");

    raf.seek(0);
    assertEquals("Hello World!", raf.readUTF());
    final RAFList<String> l1Copy = RAFList.create(raf,
        serializer, raf.getFilePointer(), 7);
    assertEquals(l1, l1Copy);
    final RAFList<String> l2Copy = RAFList.create(raf,
        serializer, l1Copy.getEndOffset(), 7);
    assertEquals(l2, l2Copy);
    raf.seek(l2Copy.getEndOffset());
    assertEquals("Goodbye World!", raf.readUTF());
    }
  }

  public void testEmptyList() throws IOException {
    final File file = File.createTempFile("asdf", "asdf");
    file.deleteOnExit();
    try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {

    raf.writeUTF("Hello World!");
    final List<String> l1 = Collections.emptyList();
    RAFList.write(raf, l1, RAFSerializer.STRING);
    raf.writeUTF("Goodbye World!");

    raf.seek(0);
    assertEquals("Hello World!", raf.readUTF());
    final RAFList<String> l1Copy = RAFList.create(raf,
        RAFSerializer.STRING, raf.getFilePointer(), 7);
    assertEquals(l1, l1Copy);
    raf.seek(l1Copy.getEndOffset());
    assertEquals("Goodbye World!", raf.readUTF());
    }
  }

}
