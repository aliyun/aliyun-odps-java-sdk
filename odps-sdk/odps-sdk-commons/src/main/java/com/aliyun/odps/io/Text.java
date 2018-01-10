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

package com.aliyun.odps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

/**
 * Text 提供了字符串的 {@link Writable} 和 {@link WritableComparable} 的实现.
 * 
 * <p>
 * Text 内部使用字节数组保存字符串，并采用 UTF-8 编码，提供了序列化、反序列化以及字节级别的字符串比较方法。
 * 
 * <p>
 * 另外，Text 也提供了一组丰富的字符串处理函数。
 */
public class Text extends BinaryComparable implements
    WritableComparable<BinaryComparable> {

  private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal<CharsetEncoder>() {
    protected CharsetEncoder initialValue() {
      return Charset.forName("UTF-8").newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
    }
  };

  private static ThreadLocal<CharsetDecoder> DECODER_FACTORY = new ThreadLocal<CharsetDecoder>() {
    protected CharsetDecoder initialValue() {
      return Charset.forName("UTF-8").newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT);
    }
  };

  private static final byte[] EMPTY_BYTES = new byte[0];

  private byte[] bytes;
  private int length;

  /**
   * 默认构造空串的 Text.
   */
  public Text() {
    bytes = EMPTY_BYTES;
  }

  /**
   * 给定字符串构造一个 Text.
   * 
   * @param string
   */
  public Text(String string) {
    set(string);
  }

  /**
   * 从另一个 Text 对象构造 Text，内容会做拷贝.
   * 
   * @param string
   */
  public Text(Text utf8) {
    set(utf8);
  }

  /**
   * 给定字节数组构造 Text
   * 
   * @param utf8
   */
  public Text(byte[] utf8) {
    set(utf8);
  }

  /**
   * 返回存储字符串内容的字节数组，注意字节数组的有效内容是：0 ~ {@link #getLength()}.
   * 
   * <strong><font color="red"> 注意：字节数组的有效内容是：0 ~ {@link #getLength()}
   * ！直接使用整个字节数组可能会读到无效内容。</font></strong>
   * 
   * @see #getLength()
   */
  @Override
  public byte[] getBytes() {
    return bytes;
  }

  /**
   * 获取内容长度，单位：字节
   * 
   * @see #getBytes()
   */
  @Override
  public int getLength() {
    return length;
  }

  /**
   * 返回给定位置 position 的 Unicode 32位标量值（Unicode scalar value）.
   * 
   * <p>
   * Text 内部使用字节数组保存字符串，并采用 UTF-8 编码，charAt 方法将字节数组在 position 位置的 UTF-8 字符转换为
   * Unicode 32位标量值并返回
   * 
   * @see #getBytes()
   * @see #getLength()
   * @return position 位置的 Unicode 32位标量值（Unicode scalar value），如果 position
   *         无效（position < 0 或 position >= {@link #getLength()}），则返回-1
   */
  public int charAt(int position) {
    // FIXME: should >= ?
    if (position > this.length)
      return -1; // too long
    if (position < 0)
      return -1; // duh.

    ByteBuffer bb = (ByteBuffer) ByteBuffer.wrap(bytes).position(position);
    return bytesToCodePoint(bb.slice());
  }

  /**
   * 查找子串出现的位置.
   * 
   * <p>
   * 等价于 find(what, 0);
   * 
   * @return 如果找到，返回子串第一次出现在字节数组的位置，如果为找到，返回-1
   */
  public int find(String what) {
    return find(what, 0);
  }

  /**
   * 从某个起始位置开始查找子串出现的位置
   * 
   * @return 如果找到，返回子串第一次出现在字节数组的位置，如果为找到，返回-1
   */
  public int find(String what, int start) {
    try {
      ByteBuffer src = ByteBuffer.wrap(this.bytes, 0, this.length);
      ByteBuffer tgt = encode(what);
      byte b = tgt.get();
      src.position(start);

      while (src.hasRemaining()) {
        if (b == src.get()) { // matching first byte
          src.mark(); // save position in loop
          tgt.mark(); // save position in target
          boolean found = true;
          int pos = src.position() - 1;
          while (tgt.hasRemaining()) {
            if (!src.hasRemaining()) { // src expired first
              tgt.reset();
              src.reset();
              found = false;
              break;
            }
            if (!(tgt.get() == src.get())) {
              tgt.reset();
              src.reset();
              found = false;
              break; // no match
            }
          }
          if (found)
            return pos;
        }
      }
      return -1; // not found
    } catch (CharacterCodingException e) {
      // can't get here
      e.printStackTrace();
      return -1;
    }
  }

  /**
   * 给定 String 设置字符串内容.
   * 
   * @param string
   */
  public void set(String string) {
    try {
      ByteBuffer bb = encode(string, true);
      bytes = bb.array();
      length = bb.limit();
    } catch (CharacterCodingException e) {
      throw new RuntimeException("Should not have happened " + e.toString());
    }
  }

  /**
   * 给定 UTF-8 编码数组设置字符串内容.
   * 
   * @param utf8
   */
  public void set(byte[] utf8) {
    set(utf8, 0, utf8.length);
  }

  /**
   * 拷贝另个 Text 的内容.
   * 
   * @param other
   */
  public void set(Text other) {
    set(other.getBytes(), 0, other.getLength());
  }

  /**
   * 设置字符串内容.
   * 
   * @param utf8
   *          待拷贝内容的字节数组
   * @param start
   *          待拷贝内容的起始位置
   * @param len
   *          待拷贝内容的长度
   */
  public void set(byte[] utf8, int start, int len) {
    setCapacity(len, false);
    System.arraycopy(utf8, start, bytes, 0, len);
    this.length = len;
  }

  /**
   * 追加字符串内容.
   * 
   * @param utf8
   *          待追加内容的字节数组
   * @param start
   *          待追加内容的起始位置
   * @param len
   *          待追加内容的长度
   */
  public void append(byte[] utf8, int start, int len) {
    setCapacity(length + len, true);
    System.arraycopy(utf8, start, bytes, length, len);
    length += len;
  }

  /**
   * 清空字符串，清空后 {@link #getLength()} 返回0
   */
  public void clear() {
    length = 0;
  }

  /*
   * Sets the capacity of this Text object to <em>at least</em> <code>len</code>
   * bytes. If the current buffer is longer, then the capacity and existing
   * content of the buffer are unchanged. If <code>len</code> is larger than the
   * current capacity, the Text object's capacity is increased to match.
   * 
   * @param len the number of bytes we need
   * 
   * @param keepData should the old data be kept
   */
  private void setCapacity(int len, boolean keepData) {
    if (bytes == null || bytes.length < len) {
      byte[] newBytes = new byte[len];
      if (bytes != null && keepData) {
        System.arraycopy(bytes, 0, newBytes, 0, length);
      }
      bytes = newBytes;
    }
  }

  /**
   * Convert text back to string
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    try {
      return decode(bytes, 0, length);
    } catch (CharacterCodingException e) {
      throw new RuntimeException("Should not have happened " + e.toString());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int newLength = WritableUtils.readVInt(in);
    setCapacity(newLength, false);
    in.readFully(bytes, 0, newLength);
    length = newLength;
  }

  public void readFields(ByteBuffer bf) {
    int newLength = bf.getInt();
    setCapacity(newLength, false);
    bf.get(bytes, 0, newLength);
    length = newLength;
  }

  /** Skips over one Text in the input. */
  public static void skip(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    WritableUtils.skipFully(in, length);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, length);
    out.write(bytes, 0, length);
  }

  /** Returns true iff <code>o</code> is a Text with the same contents. */
  public boolean equals(Object o) {
    if (o instanceof Text)
      return super.equals(o);
    return false;
  }

  public int hashCode() {
    return super.hashCode();
  }

  /**
   * Text 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class Comparator extends WritableComparator {

    public Comparator() {
      this(Text.class);
    }

    public Comparator(Class<? extends WritableComparable> keyClass) {
      super(keyClass);
    }

    /**
     * 基于二进制内容的 Text 对象比较函数.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int n1 = WritableUtils.decodeVIntSize(b1[s1]);
      int n2 = WritableUtils.decodeVIntSize(b2[s2]);
      return compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
    }
  }

  static {
    // register this comparator
    WritableComparator.define(Text.class, new Comparator());
  }

  // / STATIC UTILITIES FROM HERE DOWN
  /**
   * Converts the provided byte array to a String using the UTF-8 encoding. If
   * the input is malformed, replace by a default value.
   */
  public static String decode(byte[] utf8) throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8), true);
  }

  public static String decode(byte[] utf8, int start, int length)
      throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), true);
  }

  /**
   * Converts the provided byte array to a String using the UTF-8 encoding. If
   * <code>replace</code> is true, then malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the method throws a
   * MalformedInputException.
   */
  public static String decode(byte[] utf8, int start, int length,
      boolean replace) throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), replace);
  }

  private static String decode(ByteBuffer utf8, boolean replace)
      throws CharacterCodingException {
    CharsetDecoder decoder = DECODER_FACTORY.get();
    if (replace) {
      decoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPLACE);
      decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    String str = decoder.decode(utf8).toString();
    // set decoder back to its default value: REPORT
    if (replace) {
      decoder.onMalformedInput(CodingErrorAction.REPORT);
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return str;
  }

  /**
   * Converts the provided String to bytes using the UTF-8 encoding. If the
   * input is malformed, invalid chars are replaced by a default value.
   * 
   * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
   *         ByteBuffer.limit()
   */

  public static ByteBuffer encode(String string)
      throws CharacterCodingException {
    return encode(string, true);
  }

  /**
   * Converts the provided String to bytes using the UTF-8 encoding. If
   * <code>replace</code> is true, then malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the method throws a
   * MalformedInputException.
   * 
   * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
   *         ByteBuffer.limit()
   */
  public static ByteBuffer encode(String string, boolean replace)
      throws CharacterCodingException {
    CharsetEncoder encoder = ENCODER_FACTORY.get();
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPLACE);
      encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    ByteBuffer bytes = encoder.encode(CharBuffer.wrap(string.toCharArray()));
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPORT);
      encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return bytes;
  }

  /**
   * Read a UTF8 encoded string from in
   */
  public static String readString(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    return decode(bytes);
  }

  /**
   * Write a UTF8 encoded string to out
   */
  public static int writeString(DataOutput out, String s) throws IOException {
    ByteBuffer bytes = encode(s);
    int length = bytes.limit();
    WritableUtils.writeVInt(out, length);
    out.write(bytes.array(), 0, length);
    return length;
  }

  // //// states for validateUTF8

  private static final int LEAD_BYTE = 0;

  private static final int TRAIL_BYTE_1 = 1;

  private static final int TRAIL_BYTE = 2;

  /**
   * Check if a byte array contains valid utf-8
   * 
   * @param utf8
   *          byte array
   * @throws MalformedInputException
   *           if the byte array contains invalid utf-8
   */
  public static void validateUTF8(byte[] utf8) throws MalformedInputException {
    validateUTF8(utf8, 0, utf8.length);
  }

  /**
   * Check to see if a byte array is valid utf-8
   * 
   * @param utf8
   *          the array of bytes
   * @param start
   *          the offset of the first byte in the array
   * @param len
   *          the length of the byte sequence
   * @throws MalformedInputException
   *           if the byte array contains invalid bytes
   */
  public static void validateUTF8(byte[] utf8, int start, int len)
      throws MalformedInputException {
    int count = start;
    int leadByte = 0;
    int length = 0;
    int state = LEAD_BYTE;
    while (count < start + len) {
      int aByte = ((int) utf8[count] & 0xFF);

      switch (state) {
      case LEAD_BYTE:
        leadByte = aByte;
        length = bytesFromUTF8[aByte];

        switch (length) {
        case 0: // check for ASCII
          if (leadByte > 0x7F)
            throw new MalformedInputException(count);
          break;
        case 1:
          if (leadByte < 0xC2 || leadByte > 0xDF)
            throw new MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        case 2:
          if (leadByte < 0xE0 || leadByte > 0xEF)
            throw new MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        case 3:
          if (leadByte < 0xF0 || leadByte > 0xF4)
            throw new MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        default:
          // too long! Longest valid UTF-8 is 4 bytes (lead + three)
          // or if < 0 we got a trail byte in the lead byte position
          throw new MalformedInputException(count);
        } // switch (length)
        break;

      case TRAIL_BYTE_1:
        if (leadByte == 0xF0 && aByte < 0x90)
          throw new MalformedInputException(count);
        if (leadByte == 0xF4 && aByte > 0x8F)
          throw new MalformedInputException(count);
        if (leadByte == 0xE0 && aByte < 0xA0)
          throw new MalformedInputException(count);
        if (leadByte == 0xED && aByte > 0x9F)
          throw new MalformedInputException(count);
        // falls through to regular trail-byte test!!
      case TRAIL_BYTE:
        if (aByte < 0x80 || aByte > 0xBF)
          throw new MalformedInputException(count);
        if (--length == 0) {
          state = LEAD_BYTE;
        } else {
          state = TRAIL_BYTE;
        }
        break;
      } // switch (state)
      count++;
    }
  }

  /**
   * Magic numbers for UTF-8. These are the number of bytes that <em>follow</em>
   * a given lead byte. Trailing bytes have the value -1. The values 4 and 5 are
   * presented in this table, even though valid UTF-8 cannot include the five
   * and six byte sequences.
   */
  static final int[] bytesFromUTF8 = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0,
      0,
      0,
      0,
      0,
      // trail bytes
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2,
      2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4,
      4, 5, 5, 5, 5 };

  /**
   * Returns the next code point at the current position in the buffer. The
   * buffer's position will be incremented. Any mark set on this buffer will be
   * changed by this method!
   */
  public static int bytesToCodePoint(ByteBuffer bytes) {
    bytes.mark();
    byte b = bytes.get();
    bytes.reset();
    int extraBytesToRead = bytesFromUTF8[(b & 0xFF)];
    if (extraBytesToRead < 0)
      return -1; // trailing byte!
    int ch = 0;

    switch (extraBytesToRead) {
    case 5:
      ch += (bytes.get() & 0xFF);
      ch <<= 6; /* remember, illegal UTF-8 */
    case 4:
      ch += (bytes.get() & 0xFF);
      ch <<= 6; /* remember, illegal UTF-8 */
    case 3:
      ch += (bytes.get() & 0xFF);
      ch <<= 6;
    case 2:
      ch += (bytes.get() & 0xFF);
      ch <<= 6;
    case 1:
      ch += (bytes.get() & 0xFF);
      ch <<= 6;
    case 0:
      ch += (bytes.get() & 0xFF);
    }
    ch -= offsetsFromUTF8[extraBytesToRead];

    return ch;
  }

  static final int offsetsFromUTF8[] = { 0x00000000, 0x00003080, 0x000E2080,
      0x03C82080, 0xFA082080, 0x82082080 };

  /**
   * For the given string, returns the number of UTF-8 bytes required to encode
   * the string.
   * 
   * @param string
   *          text to encode
   * @return number of UTF-8 bytes required to encode
   */
  public static int utf8Length(String string) {
    CharacterIterator iter = new StringCharacterIterator(string);
    char ch = iter.first();
    int size = 0;
    while (ch != CharacterIterator.DONE) {
      if ((ch >= 0xD800) && (ch < 0xDC00)) {
        // surrogate pair?
        char trail = iter.next();
        if ((trail > 0xDBFF) && (trail < 0xE000)) {
          // valid pair
          size += 4;
        } else {
          // invalid pair
          size += 3;
          iter.previous(); // rewind one
        }
      } else if (ch < 0x80) {
        size++;
      } else if (ch < 0x800) {
        size += 2;
      } else {
        // ch < 0x10000, that is, the largest char value
        size += 3;
      }
      ch = iter.next();
    }
    return size;
  }
}
