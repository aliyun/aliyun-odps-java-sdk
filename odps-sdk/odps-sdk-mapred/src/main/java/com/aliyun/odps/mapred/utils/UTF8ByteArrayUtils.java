/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mapred.utils;

public class UTF8ByteArrayUtils {

  /**
   * Find the first occurrence of the given byte b in a UTF-8 encoded string
   *
   * @param utf
   *     a byte array containing a UTF-8 encoded string
   * @param start
   *     starting offset
   * @param end
   *     ending position
   * @param b
   *     the byte to find
   * @return position that first byte occures otherwise -1
   */
  public static int findByte(byte[] utf, int start, int end, byte b) {
    for (int i = start; i < end; i++) {
      if (utf[i] == b) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Find the first occurrence of the given bytes b in a UTF-8 encoded string
   *
   * @param utf
   *     a byte array containing a UTF-8 encoded string
   * @param start
   *     starting offset
   * @param end
   *     ending position
   * @param b
   *     the bytes to find
   * @return position that first byte occures otherwise -1
   */
  public static int findBytes(byte[] utf, int start, int end, byte[] b) {
    int matchEnd = end - b.length;
    for (int i = start; i <= matchEnd; i++) {
      boolean matched = true;
      for (int j = 0; j < b.length; j++) {
        if (utf[i + j] != b[j]) {
          matched = false;
          break;
        }
      }
      if (matched) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Find the nth occurrence of the given byte b in a UTF-8 encoded string
   *
   * @param utf
   *     a byte array containing a UTF-8 encoded string
   * @param start
   *     starting offset
   * @param length
   *     the length of byte array
   * @param b
   *     the byte to find
   * @param n
   *     the desired occurrence of the given byte
   * @return position that nth occurrence of the given byte if exists; otherwise -1
   */
  public static int findNthByte(byte[] utf, int start, int length, byte b, int n) {
    int pos = -1;
    int nextStart = start;
    for (int i = 0; i < n; i++) {
      pos = findByte(utf, nextStart, length, b);
      if (pos < 0) {
        return pos;
      }
      nextStart = pos + 1;
    }
    return pos;
  }

  /**
   * Find the nth occurrence of the given byte b in a UTF-8 encoded string
   *
   * @param utf
   *     a byte array containing a UTF-8 encoded string
   * @param b
   *     the byte to find
   * @param n
   *     the desired occurrence of the given byte
   * @return position that nth occurrence of the given byte if exists; otherwise -1
   */
  public static int findNthByte(byte[] utf, byte b, int n) {
    return findNthByte(utf, 0, utf.length, b, n);
  }

  private static enum UnescapeState {
    NORMAL, START_ESCAPE, ESCAPE_N, ESCAPE_NN, INVALID
  }

  /**
   * Do basical unescaping for field separator.
   * Only support '\t', '\ooo', '\\'.
   */
  public static String unescapeSeparator(String separator) {
    StringBuilder sb = new StringBuilder();

    UnescapeState state = UnescapeState.NORMAL;
    int num = 0;
    for (char c : separator.toCharArray()) {
      switch (state) {
        case NORMAL:
          if (c == '\\') {
            state = UnescapeState.START_ESCAPE;
          } else {
            sb.append(c);
          }
          break;
        case START_ESCAPE:
          if (c == 't') {
            sb.append('\t');
            state = UnescapeState.NORMAL;
          } else if (c == '\\') {
            sb.append('\\');
            state = UnescapeState.NORMAL;
          } else if (c >= '0' && c <= '7') {
            num = c - '0';
            state = UnescapeState.ESCAPE_N;
          } else {
            state = UnescapeState.INVALID;
          }
          break;
        case ESCAPE_N:
          if (c >= '0' && c <= '7') {
            num = num * 8 + (c - '0');
            state = UnescapeState.ESCAPE_NN;
          } else {
            state = UnescapeState.INVALID;
          }
          break;
        case ESCAPE_NN:
          if (c >= '0' && c <= '7') {
            num = num * 8 + (c - '0');
            if (num > 0x7f) {
              state = UnescapeState.INVALID;
              break;
            }
            sb.append((char) num);
            state = UnescapeState.NORMAL;
          } else {
            state = UnescapeState.INVALID;
          }
          break;
        default:
          // unreachable
          throw new IllegalStateException("Not handled unescaping state:" + state);
      }
      if (state.equals(UnescapeState.INVALID)) {
        break;
      }
    }
    if (!state.equals(UnescapeState.NORMAL)) {
      throw new IllegalArgumentException("Invalid escaping in separator:'" + separator + "'");
    }
    return sb.toString();
  }

}

