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

package com.aliyun.odps.counter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableUtils;

/**
 * Counter计数器，可以自定义Counter，用于统计计数，例如统计脏数据记录个数.
 *
 * <p>
 * Counter相关类的关系：
 * <ul>
 * <li>{@link Counters} 是 {@link CounterGroup} 的集合</li>
 * <li>{@link CounterGroup} 是 {@link Counter} 的集合</li>
 * </ul>
 * </p>
 *
 * <p>
 * 示例代码：
 *
 * <pre>
 * public class UserDefinedCounterExample {
 *   enum MyCounter {
 *     DIRTY_RECORDS
 *   }
 *
 *   public static class MyMapper extends Mapper&lt;Text, Text&gt; {
 *     Counter counter;
 *
 *     protected void setup(MapContext&lt;Text, LongWritable&gt; context)
 *         throws IOException, InterruptedException {
 *       counter = context.getCounter(MyCounter.DIRTY_RECORDS);
 *     }
 *
 *     public void map(LongWritable recordNum, Record record,
 *         MapContext&lt;Text, Text&gt; context) throws IOException,
 *         InterruptedException {
 *       if (dirty) {
 *         counter.increment(1);
 *       }
 *     }
 *   }
 *
 *   public static void main(String[] args) throws Exception {
 *    JobConf job = new JobConf();
 *    ... ...
 *    RunningJob rJob = JobClient.runJob(job);
 *    Counters counters = rJob.getCounters();
 *    long count = counters.findCounter(MyCounter.DIRTY_RECORDS).getValue();
 *  }
 * }
 * </pre>
 *
 * </p>
 *
 * @see Counters
 * @see CounterGroup
 * @see TaskAttemptContext#getCounter(Enum)
 */
public class Counter implements Writable {

  private String name;
  private String displayName;
  private long value = 0;

  /**
   * 默认Counter构造方法。
   */
  protected Counter() {
  }

  /**
   * Counter构造方法，这个方法指定Counter名称和显示名称
   *
   * @param name
   *     指定Counter名称
   * @param displayName
   *     指定Counter显示名称
   */
  protected Counter(String name, String displayName) {
    this.name = name;
    this.displayName = displayName;
  }

  /**
   * 设置counter的显示名称
   *
   * @param displayName
   *     指定Counter的显示名称
   */
  @Deprecated
  protected synchronized void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  /**
   * 获取Counter名称
   *
   * @return Counter名称
   */
  public synchronized String getName() {
    return name;
  }

  /**
   * 获取Counter的显示名称
   *
   * @return Counter显示名称
   */
  public synchronized String getDisplayName() {
    return displayName;
  }

  /**
   * 获取当前Counter计数值
   *
   * @return 当前Counter计数值
   */
  public synchronized long getValue() {
    return value;
  }

  /**
   * 设置Counter计数值
   *
   * @param value
   *     Counter计数值
   */
  public void setValue(long value) {
    this.value = value;
  }

  /**
   * 增加Counter计数值
   *
   * @param incr
   *     Counter计数值的增量
   */
  public synchronized void increment(long incr) {
    value += incr;
  }

  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof Counter) {
      synchronized (genericRight) {
        Counter right = (Counter) genericRight;
        return name.equals(right.name) && displayName.equals(right.displayName)
               && value == right.value;
      }
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    return name.hashCode() + displayName.hashCode();
  }

  /**
   * 从{@link DataInput}读取Counter信息
   */
  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    name = Text.readString(in);
    if (in.readBoolean()) {
      displayName = Text.readString(in);
    } else {
      displayName = name;
    }
    value = WritableUtils.readVLong(in);
  }

  /**
   * 写当前Counter到{@link DataOutput}
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    boolean distinctDisplayName = !name.equals(displayName);
    out.writeBoolean(distinctDisplayName);
    if (distinctDisplayName) {
      Text.writeString(out, displayName);
    }
    WritableUtils.writeVLong(out, value);
  }
}
