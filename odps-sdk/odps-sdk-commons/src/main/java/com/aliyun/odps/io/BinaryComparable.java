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

/**
 * 提供二进制内容的比较函数和哈希函数，在子类 {@link Text} 和 {@link BytesWritable} 中会使用到.
 */
public abstract class BinaryComparable implements Comparable<BinaryComparable> {

  /**
   * 获取二进制内容长度的虚拟方法，子类必须实现.
   *
   * @return 二进制内容长度，单位：字节
   */
  public abstract int getLength();

  /**
   * 获取二进制内容数组的虚拟方法，有效数据范围：[0, {@link #getLength()})，子类必须实现.
   *
   * @return 二进制内容数组
   */
  public abstract byte[] getBytes();

  /**
   * 比较两个二进制内容，二进制内容数组和长度由 {@link #getBytes()} 和 {@link #getLength()} 获得.
   *
   * <p>
   * 直接使用
   * {@link WritableComparator#compareBytes(byte[], int, int, byte[], int, int)}
   * 进行比较
   */
  @Override
  public int compareTo(BinaryComparable other) {
    if (this == other) {
      return 0;
    }
    return WritableComparator.compareBytes(getBytes(), 0, getLength(),
                                           other.getBytes(), 0, other.getLength());
  }

  /**
   * 比较两份二进制内容
   *
   * <p>
   * 直接使用
   * {@link WritableComparator#compareBytes(byte[], int, int, byte[], int, int)}
   * 进行比较
   *
   * @param other
   *     二进制内容数组
   * @param off
   *     内容偏移
   * @param len
   *     内容长度
   * @return 比较结果
   */
  public int compareTo(byte[] other, int off, int len) {
    return WritableComparator.compareBytes(getBytes(), 0, getLength(), other,
                                           off, len);
  }

  /**
   * 判断两份二进制内容是否相同.
   *
   * @param other
   * @return 如果两个都是 BinaryComparable 且内容相同，返回true，否则返回false
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof BinaryComparable)) {
      return false;
    }
    BinaryComparable that = (BinaryComparable) other;
    if (this.getLength() != that.getLength()) {
      return false;
    }
    return this.compareTo(that) == 0;
  }

  /**
   * 计算二进制内容的哈希值.
   *
   * <p>
   * 直接使用 {@link WritableComparator#hashBytes(byte[], int)进行计算
   */
  @Override
  public int hashCode() {
    return WritableComparator.hashBytes(getBytes(), getLength());
  }

}
