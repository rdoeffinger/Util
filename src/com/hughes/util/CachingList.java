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

package com.hughes.util;

import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;

public class CachingList<T> extends AbstractList<T> implements RandomAccess {
  
  private final List<T> list;
  private final ChunkedList<T> chunked;
  private final int size;
  
  private final LRUCacheMap<Integer, T> cache;

  public CachingList(final List<T> list, final int cacheSize) {
    this.list = list;
    // Use chunked interface if available and chunk size is
    // reasonable. Limits are pure guesswork.
    if (list instanceof ChunkedList &&
        ((ChunkedList<T>)list).getMaxChunkSize() > 1 &&
        ((ChunkedList<T>)list).getMaxChunkSize() < cacheSize / 16) {
        chunked = (ChunkedList<T>)list;
    } else {
        chunked = null;
    }
    size = list.size();
    cache = new LRUCacheMap<Integer, T>(cacheSize);
  }
  
  public static <T> CachingList<T> create(final List<T> list, final int cacheSize) {
    return new CachingList<T>(list, cacheSize);
  }

  public static <T> CachingList<T> createFullyCached(final List<T> list) {
    return new CachingList<T>(list, list.size());
  }

  @Override
  public T get(int i) {
    T t = cache.get(i);
    if (t == null) {
      if (chunked != null) {
        int start = chunked.getChunkStart(i);
        List<T> chunk = chunked.getChunk(start);
        for (int idx = 0; idx < chunk.size(); ++idx) {
            cache.put(start + idx, chunk.get(idx));
        }
        t = chunk.get(i - start);
      } else {
        t = list.get(i);
        cache.put(i, t);
      }
    }
    return t;
  }

  @Override
  public int size() {
    return size;
  }

}
