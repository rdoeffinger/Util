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

public class UniformRAFListTest extends TestCase {


  public void testFileList() throws IOException {
    final RAFSerializer<String> serializer = RAFSerializer.STRING;
    
    final File file = File.createTempFile("asdf", "asdf");
    file.deleteOnExit();
    try (final RandomAccessFile raf = new RandomAccessFile(file, "rw")) {

    raf.writeUTF("Hello World!");
    final List<String> l1 = Arrays.asList("1aaa", "1bca", "1def");
    final List<String> l2 = Arrays.asList("2aabc", "2abcd", "2adef");
    UniformRAFList.write(raf, l1, serializer, 6);
    UniformRAFList.write(raf, l2, serializer, 7);
    raf.writeUTF("Goodbye World!");

    raf.seek(0);
    assertEquals("Hello World!", raf.readUTF());
    final UniformRAFList<String> l1Copy = UniformRAFList.create(raf,
        serializer, raf.getFilePointer());
    assertEquals(l1, l1Copy);
    final UniformRAFList<String> l2Copy = UniformRAFList.create(raf,
        serializer, l1Copy.getEndOffset());
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
