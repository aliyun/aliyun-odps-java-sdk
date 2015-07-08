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

/**
 * Writable 定义了基于 {@link DataInput} 和 {@link DataOutput} 的序列化和反序列化接口.
 *
 * <p>
 * {@link com.aliyun.odps.Record} 列值要求实现 Writable 接口。
 * </p>
 *
 * <p>
 * {@link com.aliyun.odps.mapreduce.Mapper} 输出的 Key/Value 要求实现 Writable 接口。
 * </p>
 *
 * <p>
 * 用户可以实现 Writable 接口，代码示例： <blockquote>
 *
 * <pre>
 * public class MyWritable implements Writable {
 *   // Some data
 *   private int counter;
 *   private long timestamp;
 *
 *   public void write(DataOutput out) throws IOException {
 *     out.writeInt(counter);
 *     out.writeLong(timestamp);
 *   }
 *
 *   public void readFields(DataInput in) throws IOException {
 *     counter = in.readInt();
 *     timestamp = in.readLong();
 *   }
 *
 *   public static MyWritable read(DataInput in) throws IOException {
 *     MyWritable w = new MyWritable();
 *     w.readFields(in);
 *     return w;
 *   }
 * }
 * </pre>
 *
 * </blockquote>
 * </p>
 */
public interface Writable {

  /**
   * 序列化到指定的 {@link DataOutput} out.
   *
   * @param out
   * @throws IOException
   */
  void write(DataOutput out) throws IOException;

  /**
   * 从指定的 {@link DataInput} in 反序列化.
   *
   * @param in
   * @throws IOException
   */
  void readFields(DataInput in) throws IOException;
}
