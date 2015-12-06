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
import java.io.IOException;
import java.util.List;

import com.hughes.util.IndexedObject;

public class IndexedObjectSerializer<T extends IndexedObject> implements RAFSerializer<T> {
  
  private final List<T> list;
  
  public IndexedObjectSerializer(final List<T> list) {
    this.list = list;
  }

  @Override
  public T read(DataInput raf) throws IOException {
    final int index = raf.readInt();
    return list.get(index);
  }

  @Override
  public void write(DataOutput raf, T t) throws IOException {
    raf.writeInt(t.index());
  }

}
