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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class StringUtil {

    static final Pattern WHITESPACE = Pattern.compile("\\s+");

    public static String normalizeWhitespace(final String s) {
        return WHITESPACE.matcher(s.trim()).replaceAll(" ");
    }

    public static String reverse(final String s) {
        final StringBuilder builder = new StringBuilder(s);
        for (int i = 0; i < s.length(); ++i) {
            builder.setCharAt(i, s.charAt(s.length() - 1 - i));
        }
        return builder.toString();
    }

    public static String flipCase(final String s) {
        final StringBuilder builder = new StringBuilder(s);
        for (int i = 0; i < s.length(); ++i) {
            char c = builder.charAt(i);
            if (Character.isUpperCase(c)) {
                c = Character.toLowerCase(c);
            } else if (Character.isLowerCase(c)) {
                c = Character.toUpperCase(c);
            }
            builder.setCharAt(i, c);
        }
        return builder.toString();
    }

    public static String longestCommonSubstring(final String s1, final String s2) {
        for (int i = 0; i < s1.length() && i < s2.length(); i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                return s1.substring(0, i);
            }
        }
        return s1.length() < s2.length() ? s1 : s2;
    }

    public static String remove(final StringBuilder s, final Pattern start,
            final Pattern end, final boolean includeEnd) {
        final Matcher startMatcher = start.matcher(s);
        if (!startMatcher.find()) {
            return null;
        }
        final int startIndex = startMatcher.start();

        final Matcher endMatcher = end.matcher(s);
        endMatcher.region(startMatcher.end(), s.length());
        final int endIndex;
        if (endMatcher.find()) {
            endIndex = includeEnd ? endMatcher.end() : endMatcher.start();
        } else {
            endIndex = s.length();
        }

        final String result = s.substring(startIndex, endIndex);
        s.replace(startIndex, endIndex, "");
        return result;
    }

    public static StringBuilder removeAll(final StringBuilder s, final Pattern start,
            final Pattern end) {
        while (remove(s, start, end, true) != null)
            ;
        return s;
    }

    public static StringBuilder appendLine(final StringBuilder s, final CharSequence line) {
        if (s.length() > 0) {
            s.append("\n");
        }
        s.append(line);
        return s;
    }

    public static int nestedIndexOf(final String s, final int startPos, final String open,
            final String close) {
        int depth = 0;
        for (int i = startPos; i < s.length();) {
            if (s.startsWith(close, i)) {
                if (depth == 0) {
                    return i;
                } else {
                    --depth;
                    i += close.length();
                }
            } else if (s.startsWith(open, i)) {
                ++depth;
                i += open.length();
            } else {
                ++i;
            }
        }
        return -1;
    }

    public static int safeIndexOf(String s, String search) {
        final int i = s.indexOf(search);
        if (i == -1) {
            return s.length();
        }
        return i;
    }

    public static String readToString(final InputStream inputStream) {
        return new Scanner(inputStream).useDelimiter("\\A").next();
    }

    public static String escapeUnicodeToPureHtml(final String s) {
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < s.codePointCount(0, s.length()); ++i) {
            final int codePoint = s.codePointAt(i);
            if (codePoint == (char) codePoint && isPureHtml((char) codePoint)) {
                result.append(Character.valueOf((char) codePoint));
            } else {
                result.append(String.format(String.format("&#x%x;", codePoint)));
            }
        }
        return result.toString();
    }

    public static final Pattern ALL_ASCII = Pattern.compile("[\\p{ASCII}]*");

    public static boolean isAscii(final String s) {
        return ALL_ASCII.matcher(s).matches();
    }

    public static boolean isLatinLetter(final char c) {
        if (c >= 'A' && c <= 'Z') {
            return true;
        }
        return c >= 'a' && c <= 'z';
    }

    public static boolean isPureHtml(final char c) {
        return c <= 127 && c != '<' && c != '>' && c != '&' && c != '\'' && c != '"';
    }

    public static boolean isDigit(final char c) {
        return c >= '0' && c <= '9';
    }
    
    public static boolean isDigits(String name) {
        for (int i = 0; i < name.length(); ++i) {
            if (!isDigit(name.charAt(i))) {
                return false;
            }
        }
        return true;
    }


    public static boolean isUnreservedUrlCharacter(final char c) {
        if (isLatinLetter(c)) {
            return true;
        }
        if (isDigit(c)) {
            return true;
        }
        return c == '-' || c == '_' || c == '.' || c == '~';
    }

    public static final Charset UTF8 = Charset.forName("UTF-8");

    public static String encodeForUrl(final String s) {
        final StringBuilder result = new StringBuilder();
        byte[] bytes;
        try {
            bytes = s.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < bytes.length; ++i) {
            if (isUnreservedUrlCharacter((char) bytes[i])) {
                result.append((char) bytes[i]);
            } else if (bytes[i] == ' ') {
                result.append('+');
            } else {
                result.append(String.format("%%%02X", bytes[i]));
            }
        }
        return result.toString();
    }

    public static String decodeFromUrl(final String s) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 0; i < s.length(); ++i) {
            if (s.charAt(i) == '%') {
                bytes.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
                i += 2;
            } else if (s.charAt(i) == '+') {
                bytes.write(' ');
            } else {
                bytes.write(s.charAt(i));
            }
        }
        try {
            return bytes.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static final byte[] zipBytes(final byte[] bytes) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final GZIPOutputStream out = new GZIPOutputStream(baos);
        out.write(bytes);
        out.close();
        return baos.toByteArray();
    }

    public static final void unzipFully(final byte[] inBytes, final byte[] outBytes)
            throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(inBytes);
        final GZIPInputStream in = new GZIPInputStream(bais);
        int numRead = 0;
        while ((numRead += in.read(outBytes, numRead, outBytes.length - numRead)) < outBytes.length) {
            // Keep going.
        }
    }

    public static boolean isNullOrEmpty(final String s) {
        return s == null || s.length() == 0;
    }

    public static String replaceLast(String s, String search, String replaceWith) {
        final int i = s.lastIndexOf(search);
        if (i != -1) {
            return s.substring(0, i) + replaceWith + s.substring(i + search.length(), s.length());
        }
        return s;
    }


}
