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
import java.lang.reflect.Constructor;

public interface RAFSerializer<T> {

    void write(final DataOutput raf, final T t) throws IOException;

    T read(final DataInput raf) throws IOException;

    RAFSerializer<String> STRING = new RAFSerializer<String>() {
        @Override
        public void write(DataOutput raf, String t) throws IOException {
            raf.writeUTF(t);
        }

        @Override
        public String read(DataInput raf) throws IOException {
            return raf.readUTF();
        }
    };

    // Serializes any class with a write method and the proper constructor.
    final class RAFSerializableSerializer<T extends RAFSerializable<T>> implements RAFSerializer<T> {
        private final Constructor<T> constructor;

        public RAFSerializableSerializer(final Class<T> clazz) {
            try {
                this.constructor = clazz.getConstructor(DataInput.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void write(DataOutput raf, T t) throws IOException {
            t.write(raf);
        }

        @Override
        public T read(DataInput raf) {
            try {
                return constructor.newInstance(raf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
