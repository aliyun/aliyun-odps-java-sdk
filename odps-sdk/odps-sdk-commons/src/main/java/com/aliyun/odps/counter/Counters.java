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
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;

/**
 * Counters 由多个 {@link CounterGroup} 组成，保存了所有的 Counter 的信息.
 *
 * @see Counter
 * @see CounterGroup
 * @see TaskAttemptContext#getCounter(String, String)
 * @see RunningJob#getCounters()
 */
public class Counters implements Iterable<CounterGroup>, Writable {

  /**
   * A cache from enum values to the associated counter. Dramatically speeds up
   * typical usage.
   */
  private Map<Enum<?>, Counter> cache = new IdentityHashMap<Enum<?>, Counter>();

  private TreeMap<String, CounterGroup> groups = new TreeMap<String, CounterGroup>();

  public Counters() {
  }

  public Counters(Counters counters) {
  }

  /**
   * 查找 Counter，给定 Counter 组名和 Counter 名
   *
   * @param groupName
   *     Counter 组名
   * @param counterName
   *     Counter 名称
   * @return 查找到的 {@link Counter} 对象
   */
  public Counter findCounter(String groupName, String counterName) {
    CounterGroup grp = getGroup(groupName);
    return grp.findCounter(counterName);
  }

  /**
   * 查找 Counter，给定 enum 对象.
   *
   * @param key
   *     enum 对象
   * @return 查找到的 {@link Counter} 对象
   */
  public synchronized Counter findCounter(Enum<?> key) {
    Counter counter = cache.get(key);
    if (counter == null) {
      counter = findCounter(key.getDeclaringClass().getName(), key.toString());
      cache.put(key, counter);
    }
    return counter;
  }

  /**
   * 返回 Counter 组名的集合
   *
   * @return Counter 组名的集合
   */
  public synchronized Collection<String> getGroupNames() {
    return groups.keySet();
  }

  @Override
  public Iterator<CounterGroup> iterator() {
    return groups.values().iterator();
  }

  /**
   * 获取给定名字的 Counter 组，若不存在此 Counter 组，则新建一个.
   *
   * @param groupName
   *     Counter 组名
   * @return Counter 组
   */
  public synchronized CounterGroup getGroup(String groupName) {
    CounterGroup grp = groups.get(groupName);
    if (grp == null) {
      grp = new CounterGroup(groupName);
      groups.put(groupName, grp);
    }
    return grp;
  }

  /**
   * 返回Counter的个数
   *
   * @return Counter的个数
   */
  public synchronized int countCounters() {
    int result = 0;
    for (CounterGroup group : this) {
      result += group.size();
    }
    return result;
  }

  /**
   * 返回Counters的字符串形式.
   *
   * @return Counters的字符串形式
   */
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("User defined counters: "
                                         + countCounters());
    for (CounterGroup group : this) {
      sb.append("\n\t" + group.getDisplayName());
      for (Counter counter : group) {
        sb.append("\n\t\t" + counter.getDisplayName() + "="
                  + counter.getValue());
      }
    }
    return sb.toString();
  }

  /**
   * 将传入的 Counters 里的 Counter 值与本 Counters 的Counter值相加
   *
   * @param other
   *     待加的Counters
   */
  public synchronized void incrAllCounters(Counters other) {
    for (Map.Entry<String, CounterGroup> rightEntry : other.groups.entrySet()) {
      CounterGroup left = groups.get(rightEntry.getKey());
      CounterGroup right = rightEntry.getValue();
      if (left == null) {
        left = new CounterGroup(right.getName(), right.getDisplayName());
        groups.put(rightEntry.getKey(), left);
      }
      left.incrAllCounters(right);
    }
  }

  public boolean equals(Object genericRight) {
    if (genericRight instanceof Counters) {
      Iterator<CounterGroup> right = ((Counters) genericRight).groups.values()
          .iterator();
      Iterator<CounterGroup> left = groups.values().iterator();
      while (left.hasNext()) {
        if (!right.hasNext() || !left.next().equals(right.next())) {
          return false;
        }
      }
      return !right.hasNext();
    }
    return false;
  }

  public int hashCode() {
    return groups.hashCode();
  }

  /**
   * 输出到 {@link DataOutput}
   */
  @Override
  public synchronized void write(DataOutput out) throws IOException {
    out.writeInt(groups.size());
    for (CounterGroup group : groups.values()) {
      Text.writeString(out, group.getName());
      group.write(out);
    }
  }

  /**
   * 从 {@link DataInput} 读取
   */
  @Override
  public synchronized void readFields(DataInput in) throws IOException {
    cache.clear();
    int numClasses = in.readInt();
    groups.clear();
    while (numClasses-- > 0) {
      String groupName = Text.readString(in);
      CounterGroup group = new CounterGroup(groupName);
      group.readFields(in);
      groups.put(groupName, group);
    }
  }
}
