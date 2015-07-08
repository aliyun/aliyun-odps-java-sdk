/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * BlobDataInputStream是blob列类型的读取流，只能通过
 * {@link InputBlobWritable#getInputStream()} 接口获得该对象。
 *
 * <p>
 * BlobDataInputStream 继承自 {@link DataInputStream}，提供了 {@link DataInput}的实现。
 *
 * <p>
 * 代码示例:
 *
 * <pre>
 * BlobDataInputStream in = inputBlobWritable.getInputStream();
 * 使用in.read读取字节数组
 * 使用in.readType读取java基本数据类型
 * in.close();
 * </pre>
 *
 * @see DataInputStream
 * @see InputBlobWritable#getInputStream()
 */
public abstract class BlobDataInputStream extends DataInputStream {

  /**
   * BlobDataInputStream在seek时的位置标志
   *
   * <pre>
   * SET 起始位置
   * CUR 当前位置
   * END 末尾位置
   * </pre>
   */
  public enum SeekFlag {
    SET, CUR, END,
  }

  ;

  /**
   * 构建BlobDataInputStream对象
   *
   * @param in
   *     基本的数据输入流
   */
  public BlobDataInputStream(InputStream in) {
    super(in);
  }

  /**
   * 将BlobDataInputStream的位置定位到指定位置
   *
   * @param offset
   *     偏移
   * @param seekFlag
   *     相对位置
   * @throws IOException
   */
  public abstract void seek(long offset, SeekFlag seekFlag) throws IOException;

  /**
   * 获取当前位置
   *
   * @return BlobDataInputStream的当前位置
   * @throws IOException
   */
  public abstract long getPos() throws IOException;

  /**
   * 从指定位置读取指定长度的字节
   *
   * @param position
   *     BlobDataInputStream的绝对位置
   * @param buffer
   *     读取的字节要放入的缓存
   * @param offset
   *     缓存的其实位置
   * @param length
   *     待读取的字节数
   * @return 实际读取的字节数，－1表示没有可读取的字节
   * @throws IOException
   */
  public abstract int read(long position, byte[] buffer, int offset, int length)
      throws IOException;

  /**
   * 从指定位置读取length个字节
   *
   * @param position
   *     BlobDataInputStream的绝对位置
   * @param buffer
   *     读取的字节要放入的缓存
   * @param offset
   *     缓存的起始位置
   * @param length
   *     待读取的字节数
   * @throws IOException
   *     剩余的字节数小于length时
   */
  public abstract void readFully(long position, byte[] buffer, int offset,
                                 int length) throws IOException;

  /**
   * 从指定读取字节，填充满buffer传存
   *
   * @param position
   *     BlobDataInputStream的绝对位置
   * @param buffer
   *     读取的字节要放入的缓存
   * @throws IOException
   *     剩余的字节数小于传存buffer的长度时
   */
  public abstract void readFully(long position, byte[] buffer)
      throws IOException;
}
