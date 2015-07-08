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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.utils.ReflectionUtils;

/**
 * {@link Writable} 工具类.
 */
public final class WritableUtils {

  /**
   * 读取压缩的二进制输入流
   *
   * @param in
   *     二进制输入流
   * @return 读取的二进制数据
   * @throws IOException
   */
  public static byte[] readCompressedByteArray(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == -1) {
      return null;
    }
    byte[] buffer = new byte[length];
    in.readFully(buffer); // could/should use readFully(buffer,0,length)?
    GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buffer,
                                                                       0, buffer.length));
    byte[] outbuf = new byte[length];
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    int len;
    while ((len = gzi.read(outbuf, 0, outbuf.length)) != -1) {
      bos.write(outbuf, 0, len);
    }
    byte[] decompressed = bos.toByteArray();
    bos.close();
    gzi.close();
    return decompressed;
  }

  public static void skipCompressedByteArray(DataInput in) throws IOException {
    int length = in.readInt();
    if (length != -1) {
      skipFully(in, length);
    }
  }

  /**
   * 压缩写入byte数组到二进制输出流中
   *
   * @param out
   *     二进制输出流
   * @param s
   *     写入的byte数组
   * @throws IOException
   */
  public static int writeCompressedByteArray(DataOutput out, byte[] bytes)
      throws IOException {
    if (bytes != null) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      GZIPOutputStream gzout = new GZIPOutputStream(bos);
      gzout.write(bytes, 0, bytes.length);
      gzout.close();
      byte[] buffer = bos.toByteArray();
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
      /* debug only! Once we have confidence, can lose this. */
      return ((bytes.length != 0) ? (100 * buffer.length) / bytes.length : 0);
    } else {
      out.writeInt(-1);
      return -1;
    }
  }

  /**
   * 从二进制输入流中读取压缩的字符串
   *
   * @param in
   *     二进制输入流
   * @return 读取的字符串
   * @throws IOException
   */
  public static String readCompressedString(DataInput in) throws IOException {
    byte[] bytes = readCompressedByteArray(in);
    if (bytes == null) {
      return null;
    }
    return new String(bytes, "UTF-8");
  }

  /**
   * 压缩写入字符串到二进制输出流中
   *
   * @param out
   *     二进制输出流
   * @param s
   *     写入的字符串
   * @throws IOException
   */
  public static int writeCompressedString(DataOutput out, String s)
      throws IOException {
    return writeCompressedByteArray(out, (s != null) ? s.getBytes("UTF-8")
                                                     : null);
  }

  /**
   * 写入字符串到二进制输出流中
   *
   * @param out
   *     二进制输出流
   * @param s
   *     写入的字符串
   * @throws IOException
   */
  public static void writeString(DataOutput out, String s) throws IOException {
    if (s != null) {
      byte[] buffer = s.getBytes("UTF-8");
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
    } else {
      out.writeInt(-1);
    }
  }

  /**
   * 从二进制输入流中读取字符串
   *
   * @param in
   *     二进制输入流
   * @return 读取的字符串
   * @throws IOException
   */
  public static String readString(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == -1) {
      return null;
    }
    byte[] buffer = new byte[length];
    in.readFully(buffer); // could/should use readFully(buffer,0,length)?
    return new String(buffer, "UTF-8");
  }

  /**
   * 写入字符串数组到二进制输出流中
   *
   * @param out
   *     二进制输出流
   * @param s
   *     字符串数组
   * @throws IOException
   */
  public static void writeStringArray(DataOutput out, String[] s)
      throws IOException {
    out.writeInt(s.length);
    for (int i = 0; i < s.length; i++) {
      writeString(out, s[i]);
    }
  }

  /**
   * 压缩写入字符串数组到二进制输出流中
   *
   * @param out
   *     二进制输出流
   * @param s
   *     字符串数组
   * @throws IOException
   */
  public static void writeCompressedStringArray(DataOutput out, String[] s)
      throws IOException {
    if (s == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(s.length);
    for (int i = 0; i < s.length; i++) {
      writeCompressedString(out, s[i]);
    }
  }

  /**
   * 从二进制输入流中读取字符串数组
   *
   * @param in
   *     二进制输入流
   * @return 字符串数组
   * @throws IOException
   */
  public static String[] readStringArray(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == -1) {
      return null;
    }
    String[] s = new String[len];
    for (int i = 0; i < len; i++) {
      s[i] = readString(in);
    }
    return s;
  }

  /**
   * 从二进制输入流中读取压缩的字符串数组
   *
   * @param in
   *     二进制输入流
   * @return 字符串数组
   * @throws IOException
   */
  public static String[] readCompressedStringArray(DataInput in)
      throws IOException {
    int len = in.readInt();
    if (len == -1) {
      return null;
    }
    String[] s = new String[len];
    for (int i = 0; i < len; i++) {
      s[i] = readCompressedString(in);
    }
    return s;
  }

  /**
   * 打印出byte数组的字符串形式
   *
   * @param record
   *     byte数组
   */
  public static void displayByteArray(byte[] record) {
    int i;
    for (i = 0; i < record.length - 1; i++) {
      if (i % 16 == 0) {
        System.out.println();
      }
      System.out.print(Integer.toHexString(record[i] >> 4 & 0x0F));
      System.out.print(Integer.toHexString(record[i] & 0x0F));
      System.out.print(",");
    }
    System.out.print(Integer.toHexString(record[i] >> 4 & 0x0F));
    System.out.print(Integer.toHexString(record[i] & 0x0F));
    System.out.println();
  }


  /**
   * vint方式序列化一个int到二进制输出流中
   *
   * @param stream
   *     二进制输出流
   * @param i
   *     int的值
   * @throws java.io.IOException
   */
  public static void writeVInt(DataOutput stream, int i) throws IOException {
    writeVLong(stream, i);
  }

  /**
   * vlong方式序列化一个long到二进制输出流中
   *
   * @param stream
   *     二进制输出流
   * @param i
   *     long的值
   * @throws java.io.IOException
   */
  public static void writeVLong(DataOutput stream, long i) throws IOException {
    if (i >= -112 && i <= 127) {
      stream.writeByte((byte) i);
      return;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    stream.writeByte((byte) len);

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      stream.writeByte((byte) ((i & mask) >> shiftbits));
    }
  }

  /**
   * 从二进制流中读取vlong.
   *
   * @param stream
   *     输入二进制流
   * @return vlong值
   * @throws java.io.IOException
   */
  public static long readVLong(DataInput stream) throws IOException {
    byte firstByte = stream.readByte();
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len - 1; idx++) {
      byte b = stream.readByte();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * 从二进制流中读取vint.
   *
   * @param stream
   *     输入二进制流
   * @return vint值
   * @throws java.io.IOException
   */
  public static int readVInt(DataInput stream) throws IOException {
    return (int) readVLong(stream);
  }

  /**
   * 解析vint、vlong的首字节判断其是否为负值
   *
   * @param value
   *     vint、vlong的首字节
   * @return 是否为负值
   */
  public static boolean isNegativeVInt(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * 解析vint、vlong的首字节（表示其本身的长度）
   *
   * @param value
   *     vint、vlong的首字节
   * @return 字节长度(1 - 9)
   */
  public static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  /**
   * 获取变长的int长度
   *
   * @param i
   *     int值
   * @return 变长的int长度
   */
  public static int getVIntSize(long i) {
    if (i >= -112 && i <= 127) {
      return 1;
    }

    if (i < 0) {
      i ^= -1L; // take one's complement'
    }
    // find the number of bytes with non-leading zeros
    int dataBits = Long.SIZE - Long.numberOfLeadingZeros(i);
    // find the number of data bytes + length byte
    return (dataBits + 7) / 8 + 1;
  }

  /**
   * 从输入流中读取枚举值
   *
   * @param <T>
   *     Enum 枚举类型
   * @param in
   *     输入流
   * @param enumType
   *     枚举类
   * @return Enum 读取枚举值
   * @throws IOException
   */
  public static <T extends Enum<T>> T readEnum(DataInput in, Class<T> enumType)
      throws IOException {
    return T.valueOf(enumType, Text.readString(in));
  }

  /**
   * 写入枚举到输出流
   *
   * @param out
   *     输出流
   * @param enumVal
   *     枚举值
   * @throws IOException
   */
  public static void writeEnum(DataOutput out, Enum<?> enumVal)
      throws IOException {
    Text.writeString(out, enumVal.name());
  }

  /**
   * 输入流中跳过指定长度的字节
   *
   * @param in
   *     输入流
   * @param len
   *     指定字节长度
   * @throws IOException
   */
  public static void skipFully(DataInput in, int len) throws IOException {
    int total = 0;
    int cur = 0;

    while ((total < len) && ((cur = in.skipBytes(len - total)) > 0)) {
      total += cur;
    }

    if (total < len) {
      throw new IOException("Not able to skip " + len + " bytes, possibly "
                            + "due to end of input.");
    }
  }

  /**
   * 将Writable对象转化成byte数组
   *
   * @param writables
   *     Writable对象集合
   * @return 转化后二进制数据
   */
  public static byte[] toByteArray(Writable... writables) {
    final DataOutputBuffer out = new DataOutputBuffer();
    try {
      for (Writable w : writables) {
        w.write(out);
      }
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Fail to convert writables to a byte array", e);
    }
    return out.getData();
  }

  /**
   * 通过Writable序列化方式拷贝一个Writable对象.
   *
   * @param orig
   *     拷贝源对象
   * @return 拷贝对象
   * @throws IOException
   */
  public static <T extends Writable> T clone(T orig, Configuration conf) {
    DataOutputBuffer out = new DataOutputBuffer();
    DataInputBuffer in = new DataInputBuffer();
    try {
      @SuppressWarnings("unchecked")
      // Unchecked cast from Class to Class<T>
          T newInst = ReflectionUtils.newInstance((Class<T>) orig.getClass(), conf);
      orig.write(out);
      in.reset(out.getData(), out.getLength());
      newInst.readFields(in);
      return newInst;
    } catch (IOException e) {
      throw new RuntimeException("Error writing/reading clone buffer", e);
    } finally {
      try {
        out.close();
        in.close();
      } catch (IOException e) {
        throw new RuntimeException("Error writing/reading clone buffer", e);
      }
    }
  }
}
