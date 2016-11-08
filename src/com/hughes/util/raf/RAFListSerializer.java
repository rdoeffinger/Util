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

public interface RAFListSerializer<T> {

    public void write(final DataOutput raf, final T t) throws IOException;

    public T read(final DataInput raf, final int readIndex) throws IOException;

    static final class Wrapper<T> implements RAFListSerializer<T> {

        private final RAFSerializer<T> serializer;

        public Wrapper(final RAFSerializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public T read(DataInput raf, int readIndex) throws IOException {
            return serializer.read(raf);
        }

        @Override
        public void write(DataOutput raf, T t) throws IOException {
            serializer.write(raf, t);
        }

    }

}
