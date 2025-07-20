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

import java.util.Collection;

@SuppressWarnings("WeakerAccess")
public final class CollectionUtil {

    @SuppressWarnings("unused")
    public static String join(final Collection<?> list, final String inbetween) {
        final StringBuilder builder = new StringBuilder();
        for (final Object object : list) {
            if (!builder.isEmpty()) {
                builder.append(inbetween);
            }
            builder.append(object);
        }
        return builder.toString();
    }

}
