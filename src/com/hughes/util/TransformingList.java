// Copyright 2012 Google Inc. All Rights Reserved.
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

public class TransformingList<T1,T2> extends AbstractList<T2> implements RandomAccess {

    public interface Transformer<T1,T2> {
        T2 transform(T1 t1);
    }

    private final List<T1> list;
    private final Transformer<T1,T2> transformer;

    @SuppressWarnings("WeakerAccess")
    public TransformingList(final List<T1> list, final Transformer<T1, T2> transformer) {
        this.list = list;
        this.transformer = transformer;
    }

    public static <T1,T2> TransformingList<T1,T2> create(final List<T1> list, final Transformer<T1, T2> transformer) {
        return new TransformingList<>(list, transformer);
    }

    @Override
    public T2 get(int i) {
        return transformer.transform(list.get(i));
    }

    @Override
    public int size() {
        return list.size();
    }

}
