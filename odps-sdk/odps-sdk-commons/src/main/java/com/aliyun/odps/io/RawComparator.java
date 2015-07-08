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

import java.util.Comparator;

/**
 * <p>
 * RawComparator 接口定义了一个基于对象二进制表示的比较方法.
 * </p>
 *
 * @param <T>
 *     此 Comparator 可以比较的对象类型
 */
public interface RawComparator<T> extends Comparator<T> {

  /**
   * 基于对象二进制表示的比较方法.
   *
   * <p>
   * {@link Writable} 接口定义了对象序列化和反序列化的接口，序列化过程将对象内容写到内存的
   * byte数组中，本接口定义了byte数组表示的两个对象的比较方法，即对象比较转为比较 b1[s1, s1+l1) 和 b2[s2, s2+l2)
   * 的二进制内容。
   *
   * <p>
   * 这种直接基于二进制表示的比较函数在某些情况下可以做到比直接将对象反序列化后再进行比较来得高效，对于性能敏感的场景，可以提供本接口的实现。
   *
   * @param b1
   *     对象1二进制内容所在的byte数组
   * @param s1
   *     对象1二进制内容在b1的起始位置
   * @param l1
   *     对象1二进制内容长度
   * @param b2
   *     对象2二进制内容所在的byte数组
   * @param s2
   *     对象2二进制内容在b2的起始位置
   * @param l2
   *     对象2二进制内容长度
   * @return 对象1>对象2，返回1，对象1=对象2，返回0，对象1<对象2，返回-1
   */
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);

}
