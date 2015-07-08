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

package com.aliyun.odps.counter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TreeMap;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableUtils;

/**
 * Counter 组，由多个 {@link Counter} 计数器组成.
 *
 * @see Counter
 * @see Counters
 */
public class CounterGroup implements Iterable<Counter>, Writable {

  private String name;
  private String displayName;
  private TreeMap<String, Counter> counters = new TreeMap<String, Counter>();
  // Optional ResourceBundle for localization of group and counter names.
  private ResourceBundle bundle = null;

  /**
   * Returns the specified resource bundle, or throws an exception.
   *
   * @throws MissingResourceException
   *     if the bundle isn't found
   */
  private static ResourceBundle getResourceBundle(String enumClassName) {
    String bundleName = enumClassName.replace('$', '_');
    return ResourceBundle.getBundle(bundleName);
  }

  /**
   * CounterGroup构造方法，该方法设置Counter组的名称
   *
   * @param name
   *     Counter组的名称
   */
  protected CounterGroup(String name) {
    this.name = name;
    try {
      bundle = getResourceBundle(name);
    } catch (MissingResourceException neverMind) {
    }
    displayName = localize("CounterGroupName", name);
  }

  /**
   * CounterGroup构造方法，该方法设置Counter组的名称和显示名称
   *
   * @param name
   *     Counter组的名称
   * @param displayName
   *     Counter组的显示名称
   */
  protected CounterGroup(String name, String displayName) {
    this.name = name;
    this.displayName = displayName;
  }


  /**
   * 获取 Counter 组的名称
   *
   * @return Counter 组的名称
   */
  public synchronized String getName() {
    return name;
  }

  /**
   * 获取 Counter 组的显示名称
   *
   * @return Counter 组的显示名称
   */
  public synchronized String getDisplayName() {
    return displayName;
  }

  /**
   * 增加 {@link Counter} 到此 Counter 组
   *
   * @param counter
   *     待加入的{@link Counter}对象
   */
  synchronized void addCounter(Counter counter) {
    counters.put(counter.getName(), counter);
  }

  /**
   * 从 Counter 组查找 {@link Counter}对象，
   * 如果不存在，自动创建 {@link Counter}对象 并加入此 Counter 组.
   *
   * @param counterName
   *     查找的{@link Counter}名称
   * @param displayName
   *     当查找{@link Counter}不存在时，
   *     创建新{@link Counter}指定的显示名称
   * @return 查找到的{@link Counter}对象
   */
  protected Counter findCounter(String counterName, String displayName) {
    Counter result = counters.get(counterName);
    if (result == null) {
      result = new Counter(counterName, displayName);
      counters.put(counterName, result);
    }
    return result;
  }

  /**
   * 从 Counter 组查找 {@link Counter}对象，
   * 如果不存在，自动创建 {@link Counter}对象 并加入此 Counter 组.
   *
   * @param counterName
   *     查找的{@link Counter}名称
   * @return 查找到的{@link Counter}对象
   */
  public synchronized Counter findCounter(String counterName) {
    Counter result = counters.get(counterName);
    if (result == null) {
      String displayName = localize(counterName, counterName);
      result = new Counter(counterName, displayName);
      counters.put(counterName, result);
    }
    return result;
  }

  /**
   * 返回 Counter的迭代器.
   *
   * @return Counter的迭代器
   */
  public synchronized Iterator<Counter> iterator() {
    return counters.values().iterator();
  }

  /**
   * Looks up key in the ResourceBundle and returns the corresponding value. If
   * the bundle or the key doesn't exist, returns the default value.
   */
  private String localize(String key, String defaultValue) {
    String result = defaultValue;
    if (bundle != null) {
      try {
        result = bundle.getString(key);
      } catch (MissingResourceException mre) {
      }
    }
    return result;
  }

  /**
   * 返回 Counter 组包含的 Counter 数
   *
   * @return 包含的 Counter 数
   */
  public synchronized int size() {
    return counters.size();
  }

  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroup) {
      Iterator<Counter> right = ((CounterGroup) genericRight).counters.values()
          .iterator();
      Iterator<Counter> left = counters.values().iterator();
      while (left.hasNext()) {
        if (!right.hasNext() || !left.next().equals(right.next())) {
          return false;
        }
      }
      return !right.hasNext();
    }
    return false;
  }

  public synchronized int hashCode() {
    return counters.hashCode();
  }

  /**
   * 将传入的 Counter 组里的 Counter 值与本 Counter 组的Counter值相加
   *
   * @param rightGroup
   *     待加的 Counter 组
   */
  public synchronized void incrAllCounters(CounterGroup rightGroup) {
    for (Counter right : rightGroup.counters.values()) {
      Counter left = findCounter(right.getName(), right.getDisplayName());
      left.increment(right.getValue());
    }
  }

  /**
   * 将本 Counter 组中的 Counter 值置为0
   */
  public synchronized void resetAllCounters() {
    for (Counter counter : this) {
      counter.setValue(0);
    }
  }

  /**
   * 将此 Counter 组写到 {@link DataOutput}.
   */
  public synchronized void write(DataOutput out) throws IOException {
    Text.writeString(out, displayName);
    WritableUtils.writeVInt(out, counters.size());
    for (Counter counter : counters.values()) {
      counter.write(out);
    }
  }

  /**
   * 从 {@link DataInput} 读取 Counter 组.
   */
  public synchronized void readFields(DataInput in) throws IOException {
    displayName = Text.readString(in);
    counters.clear();
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      Counter counter = new Counter();
      counter.readFields(in);
      counters.put(counter.getName(), counter);
    }
  }
}
