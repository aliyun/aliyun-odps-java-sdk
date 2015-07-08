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
 * WritableComparable 接口同时继承了 {@link Writable} 和 {@link Comparable}.
 *
 * <p>
 * <code>WritableComparable</code>s 对象之间可以进行比较，{@link Mapper} 输出的 Key
 * 都需要实现此接口用于排序.
 * </p>
 *
 * <p>
 * 代码示例:
 *
 * <pre>
 * public class MyWritableComparable implements WritableComparable {
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
 *   public int compareTo(MyWritableComparable w) {
 *     int thisValue = this.value;
 *     int thatValue = ((IntWritable) o).value;
 *     return (thisValue &lt; thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
 *   }
 * }
 * </pre>
 *
 * </p>
 */
public interface WritableComparable<T> extends Writable, Comparable<T> {

}
