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

import java.util.List;

public final class ListUtil {
  
  public static final <T> T getLast(final List<T> list) {
    return list.get(list.size() - 1);
  }
  
  public static <T> T get(final List<T> list, final int index, final T defaultValue) {
    return index < list.size() ? list.get(index) : defaultValue;
  }

  public static <T> T get(final List<T> list, final int index) {
    return get(list, index, null);
  }

  public static <T> T remove(final List<T> list, final int index, final T defaultValue) {
    return index < list.size() ? list.remove(index) : defaultValue;
  }
  
  public static final <T> void swap(final List<T> list, int i1, int i2) {
    if (i1 == i2) { return; }
    T temp = list.get(i1);
    list.set(i1,list.get(i2));
    list.set(i2, temp);
  }
  
  public static final <T> T fastRemoveWithReorder(final List<T> list, final int index) {
      swap(list, index, list.size() - 1);
      return list.remove(list.size() - 1);
  }
  
}
