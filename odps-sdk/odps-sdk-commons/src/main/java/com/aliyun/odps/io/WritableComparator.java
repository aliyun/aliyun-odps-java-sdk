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
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;

import com.aliyun.odps.utils.ReflectionUtils;

/**
 * WritableComparator 提供对 {@link WritableComparable} 对象的通用比较函数.
 *
 * <p>
 * 通用比较函数使用对象的自然顺序进行排序，如果用户需要自定义对象顺序，则需要重载
 * {@link #compare(WritableComparable, WritableComparable)}方法。
 *
 * <p>
 * 如果对排序性能敏感，可以重载 {@link #compare(byte[], int, int, byte[], int, int)} 方法。
 */
@SuppressWarnings("rawtypes")
public class WritableComparator implements RawComparator {

  private static HashMap<Class, WritableComparator>
      comparators =
      new HashMap<Class, WritableComparator>();
      // registry

  /**
   * 此静态方法用于获取为类型 c 注册的 WritableComparator 实现.
   *
   * <p>
   * 此方法返回通过 {@link #define(Class, WritableComparator)} 方法为类型 c 注册的
   * WritableComparator 实现，如果没有注册，则返回 WritableComparator 这一通用实现。
   *
   * @param c
   *     待比较的 {@link WritableComparable} 类型
   * @return WritableComparator 实现
   * @see JobConf#getOutputKeyComparator()
   */
  public static synchronized WritableComparator get(
      Class<? extends WritableComparable> c) {
    WritableComparator comparator = comparators.get(c);
    if (comparator == null) {
      comparator = new WritableComparator(c, true);
    }
    return comparator;
  }

  /**
   * 此静态方法用于为指定类型注册更高效的 WritableComparator 实现，否则默认使用本实现.
   *
   * <p>
   * 注意：只能注册线程安全的比较器，注册的对象可能在多线程场景中使用
   *
   * @param c
   *     待比较的 {@link WritableComparable} 类型
   * @param comparator
   *     更高效的 WritableComparator 实现
   * @see #get(Class)
   */
  public static synchronized void define(Class c, WritableComparator comparator) {
    comparators.put(c, comparator);
  }

  private final Class<? extends WritableComparable> keyClass;
  private final WritableComparable key1;
  private final WritableComparable key2;
  private final DataInputBuffer buffer;

  /**
   * 构造函数，传入待比较的 {@link WritableComparable} 类型（通常是 {@link Mapper} 输出的 Key类型）.
   *
   * @param keyClass
   *     待比较的 {@link WritableComparable} 类型
   */
  protected WritableComparator(Class<? extends WritableComparable> keyClass) {
    this(keyClass, false);
  }

  /**
   * 构造函数，传入待比较的 Key 类型和是否创建 Key 对象
   *
   * @param keyClass
   * @param createInstances
   */
  protected WritableComparator(Class<? extends WritableComparable> keyClass,
                               boolean createInstances) {
    this.keyClass = keyClass;
    if (createInstances) {
      key1 = newKey();
      key2 = newKey();
      buffer = new DataInputBuffer();
    } else {
      key1 = key2 = null;
      buffer = null;
    }
  }

  /**
   * 返回待比较的 {@link WritableComparable} 实现类
   *
   * @return {@link WritableComparable} 实现类
   */
  public Class<? extends WritableComparable> getKeyClass() {
    return keyClass;
  }

  /**
   * 新建一个新的 {@link WritableComparable} 对象.
   *
   * @return 新创建的 {@link WritableComparable} 对象
   */
  public WritableComparable newKey() {
    return ReflectionUtils.newInstance(keyClass, null);
  }

  /**
   * 本方法是 {@link RawComparator} 的低效实现，如果能提供此方法的高效实现，请重载此方法.
   *
   * <p>
   * 本方法的默认实现是：先通过 {@link Writable#readFields(DataInput)} 将二进制表示反序列化为
   * {@link WritableComparable} 对象，然后调用
   * {@link #compare(WritableComparable, WritableComparable)} 进行比较。
   */
  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    try {
      buffer.reset(b1, s1, l1); // parse key1
      key1.readFields(buffer);

      buffer.reset(b2, s2, l2); // parse key2
      key2.readFields(buffer);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return compare(key1, key2); // compare them
  }

  /**
   * 比较两个 {@link WritableComparable} 对象.
   *
   * <p>
   * 按自然顺序比较两个 {@link WritableComparable} 对象，直接调用
   * {@link Comparable#compareTo(Object)}.
   *
   * @param a
   *     左边 {@link WritableComparable} 对象
   * @param b
   *     右边 {@link WritableComparable} 对象
   * @return a.compareTo(b)
   */
  @SuppressWarnings("unchecked")
  public int compare(WritableComparable a, WritableComparable b) {
    return a.compareTo(b);
  }

  /**
   * 重载 {@link Comparator#compare(Object, Object)} 方法，定义为 final，不允许子类重载.
   *
   * <p>
   * 本比较函数直接调用 {@link #compare(WritableComparable, WritableComparable)}.
   */
  @Override
  public final int compare(Object a, Object b) {
    return compare((WritableComparable) a, (WritableComparable) b);
  }

  /**
   * 逐字节比较两组二进制数据.
   *
   * @param b1
   * @param s1
   * @param l1
   * @param b2
   * @param s2
   * @param l2
   * @return
   * @see RawComparator#compare(byte[], int, int, byte[], int, int)
   */
  public static int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2,
                                 int l2) {
    int end1 = s1 + l1;
    int end2 = s2 + l2;
    for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
      int a = (b1[i] & 0xff);
      int b = (b2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return l1 - l2;
  }

  /**
   * 计算二进制数据的哈希值.
   *
   * @param bytes
   *     byte数组
   * @param length
   *     数据长度
   * @return 哈希值
   */
  public static int hashBytes(byte[] bytes, int length) {
    int hash = 1;
    for (int i = 0; i < length; i++) {
      hash = (31 * hash) + (int) bytes[i];
    }
    return hash;
  }

  /**
   * 从 byte 数组读取 tinyint.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容bytes[start, start]
   * @return
   */
  static byte readByte(byte[] bytes, int start) {
    return bytes[start];
  }

  /**
   * 从 byte 数组读取 short.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容bytes[start, start+1]
   * @return
   */
  static short readShort(byte[] bytes, int start) {
    return (short) (((bytes[start] & 0xff) << 8) + ((bytes[start + 1] & 0xff)));
  }

  /**
   * 从 byte 数组读取无符号 short.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容bytes[start, start+1]
   * @return
   */
  public static int readUnsignedShort(byte[] bytes, int start) {
    return (((bytes[start] & 0xff) << 8) + ((bytes[start + 1] & 0xff)));
  }

  /**
   * 从 byte 数组读取 int.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容bytes[start, start+3]
   * @return
   */
  public static int readInt(byte[] bytes, int start) {
    return (((bytes[start] & 0xff) << 24) + ((bytes[start + 1] & 0xff) << 16)
            + ((bytes[start + 2] & 0xff) << 8) + ((bytes[start + 3] & 0xff)));

  }

  /**
   * 从 byte 数组读取 float.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容bytes[start, start+3]
   * @return
   */
  public static float readFloat(byte[] bytes, int start) {
    return Float.intBitsToFloat(readInt(bytes, start));
  }

  /**
   * 从 byte 数组读取 long.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容bytes[start, start+7]
   * @return
   */
  public static long readLong(byte[] bytes, int start) {
    return ((long) (readInt(bytes, start)) << 32)
           + (readInt(bytes, start + 4) & 0xFFFFFFFFL);
  }

  /**
   * 从 byte 数组读取double.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容bytes[start, start+7]
   * @return
   */
  public static double readDouble(byte[] bytes, int start) {
    return Double.longBitsToDouble(readLong(bytes, start));
  }

  /**
   * 从byte数组读取压缩编码过的 long.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容长度依 long 值的压缩编码特征而定
   * @return
   * @throws IOException
   */
  public static long readVLong(byte[] bytes, int start) throws IOException {
    int len = bytes[start];
    if (len >= -112) {
      return len;
    }
    boolean isNegative = (len < -120);
    len = isNegative ? -(len + 120) : -(len + 112);
    if (start + 1 + len > bytes.length) {
      throw new IOException(
          "Not enough number of bytes for a zero-compressed integer");
    }
    long i = 0;
    for (int idx = 0; idx < len; idx++) {
      i = i << 8;
      i = i | (bytes[start + 1 + idx] & 0xFF);
    }
    return (isNegative ? (i ^ -1L) : i);
  }

  /**
   * 从byte数组读取压缩编码过的 int.
   *
   * @param bytes
   *     字节数组
   * @param start
   *     起始位置，读取内容长度依 int 值的压缩编码特征而定
   * @return
   * @throws IOException
   */
  public static int readVInt(byte[] bytes, int start) throws IOException {
    return (int) readVLong(bytes, start);
  }
}
