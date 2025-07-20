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

import java.io.IOException;
import java.util.regex.Pattern;

import junit.framework.TestCase;

public class StringUtilTest extends TestCase {

    public void testURLs() {
        {
            final String s = "asdf< >a%b%c<->asdf";
            final String url = "asdf%3C+%3Ea%25b%25c%3C-%3Easdf";
            assertEquals(url, StringUtil.encodeForUrl(s));
            assertEquals(s, StringUtil.decodeFromUrl(url));
        }

        {
            final String s = "röten";
            final String url = "r%C3%B6ten";
            assertEquals(url, StringUtil.encodeForUrl(s));
            assertEquals(s, StringUtil.decodeFromUrl(url));
        }

        {
            final String s = "%";
            final String url = "%25";
            assertEquals(url, StringUtil.encodeForUrl(s));
            assertEquals(s, StringUtil.decodeFromUrl(url));
        }
    }

}
