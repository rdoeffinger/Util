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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterOutputStream;

public final class StringUtil {

    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    public static String normalizeWhitespace(final String s) {
        return WHITESPACE.matcher(s.trim()).replaceAll(" ");
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
                result.append(String.format("&#x%x;", codePoint));
            }
        }
        return result.toString();
    }

    private static final Pattern ALL_ASCII = Pattern.compile("[\\p{ASCII}]*");

    @SuppressWarnings("unused")
    public static boolean isAscii(final String s) {
        return ALL_ASCII.matcher(s).matches();
    }

    private static boolean isLatinLetter(final char c) {
        return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z';
    }

    private static boolean isPureHtml(final char c) {
        return c <= 127 && c != '<' && c != '>' && c != '&' && c != '\'' && c != '"';
    }

    private static boolean isDigit(final char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isUnreservedUrlCharacter(final char c) {
        return isLatinLetter(c) || isDigit(c) || c == '-' || c == '_' || c == '.' || c == '~';
    }

    public static String encodeForUrl(final String s) {
        final StringBuilder result = new StringBuilder();
        byte[] bytes;
        bytes = s.getBytes(StandardCharsets.UTF_8);
        for (byte aByte : bytes) {
            if (isUnreservedUrlCharacter((char) aByte)) {
                result.append((char) aByte);
            } else if (aByte == ' ') {
                result.append('+');
            } else {
                result.append(String.format("%%%02X", aByte));
            }
        }
        return result.toString();
    }

    public static String decodeFromUrl(final String s) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 0; i < s.length(); ++i) {
            switch (s.charAt(i)) {
                case '%':
                    bytes.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
                    i += 2;
                    break;
                case '+':
                    bytes.write(' ');
                    break;
                default:
                    bytes.write(s.charAt(i));
                    break;
            }
        }
        try {
            return bytes.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Missing UTF-8 support?!", e);
        }
    }

    public static void writeVarInt(DataOutput f, int v) throws IOException {
        if (v < 0 || v >= 16 * 256 * 256 * 256) {
            f.writeByte(240);
            f.writeInt(v);
        } else if (v < 128) {
            f.writeByte(v);
        } else if (v < 64 * 256) {
            f.writeShort(v + 128 * 256);
        } else if (v < 32 * 256 * 256) {
            f.writeByte((v >> 16) + 192);
            f.writeShort(v);
        } else {
            f.writeShort((v >> 16) + 224 * 256);
            f.writeShort(v);
        }
    }

    public static int readVarInt(DataInput f) throws IOException {
        int v = f.readUnsignedByte();
        if (v < 128)
            return v;
        if (v < 192)
            return ((v - 128) << 8) + f.readUnsignedByte();
        if (v < 224)
            return ((v - 192) << 16) + f.readUnsignedShort();
        if (v < 240) {
            v = ((v - 224) << 16) + f.readUnsignedShort();
            return (v << 8) + f.readUnsignedByte();
        }
        return f.readInt();
    }

    public static byte[] unzipFully(final byte[] inBytes, final int size)
    throws IOException {
        if (size == -1) {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final OutputStream out = new InflaterOutputStream(baos);
            out.write(inBytes);
            out.close();
            return baos.toByteArray();
        }
        byte[] outBytes = new byte[size];
        final ByteArrayInputStream bais = new ByteArrayInputStream(inBytes);
        final InputStream in = new GZIPInputStream(bais);
        int numRead = 0;
        while ((numRead += in.read(outBytes, numRead, outBytes.length - numRead)) < outBytes.length) {
            // Keep going.
        }
        return outBytes;
    }

    @SuppressWarnings("unused")
    public static boolean isNullOrEmpty(final String s) {
        return s == null || s.length() == 0;
    }
}
