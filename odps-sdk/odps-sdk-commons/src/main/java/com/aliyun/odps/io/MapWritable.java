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
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.aliyun.odps.utils.ReflectionUtils;

/**
 * 可以用于序列化的{@link Map}，使用方法相同，可以在{@link com.aliyun.odps.mapreduce.Mapper} 中调用
 * {@link com.aliyun.odps.mapreduce.MapContext#write(Object, Object)}写出。
 */
public class MapWritable extends AbstractMapWritable implements
                                                     Map<Writable, Writable> {

  private Map<Writable, Writable> instance;

  /**
   * 默认构造函数。
   */
  public MapWritable() {
    super();
    this.instance = new HashMap<Writable, Writable>();
  }

  /**
   * 拷贝构造函数
   *
   * @param 另一个待拷贝的
   */
  public MapWritable(MapWritable other) {
    this();
    copy(other);
  }

  @Override
  public void clear() {
    instance.clear();
  }

  @Override
  public boolean containsKey(Object key) {
    return instance.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return instance.containsValue(value);
  }

  @Override
  public Set<Map.Entry<Writable, Writable>> entrySet() {
    return instance.entrySet();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof MapWritable) {
      Map map = (Map) obj;
      if (size() != map.size()) {
        return false;
      }

      return entrySet().equals(map.entrySet());
    }

    return false;
  }

  @Override
  public Writable get(Object key) {
    return instance.get(key);
  }

  @Override
  public int hashCode() {
    return 1 + this.instance.hashCode();
  }

  @Override
  public boolean isEmpty() {
    return instance.isEmpty();
  }

  @Override
  public Set<Writable> keySet() {
    return instance.keySet();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Writable put(Writable key, Writable value) {
    if (key != null) {
      addToMap(key.getClass());
    }
    if (value != null) {
      addToMap(value.getClass());
    }
    return instance.put(key, value);
  }

  @Override
  public void putAll(Map<? extends Writable, ? extends Writable> t) {
    for (Map.Entry<? extends Writable, ? extends Writable> e : t.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public Writable remove(Object key) {
    return instance.remove(key);
  }

  @Override
  public int size() {
    return instance.size();
  }

  @Override
  public Collection<Writable> values() {
    return instance.values();
  }

  // Writable

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    // Write out the number of entries in the map

    out.writeInt(instance.size());

    // Then write out each key/value pair

    for (Map.Entry<Writable, Writable> e : instance.entrySet()) {
      Writable key = e.getKey();
      if (key == null) {
        out.writeByte(getId(NullWritable.class));
      } else {
        out.writeByte(getId(key.getClass()));
        key.write(out);
      }

      Writable value = e.getValue();
      if (value == null) {
        out.writeByte(getId(NullWritable.class));
      } else {
        out.writeByte(getId(value.getClass()));
        value.write(out);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    // First clear the map. Otherwise we will just accumulate
    // entries every time this method is called.
    this.instance.clear();

    // Read the number of entries in the map

    int entries = in.readInt();

    // Then read each key/value pair

    for (int i = 0; i < entries; i++) {
      Class keyClass = getClass(in.readByte());
      Writable key = null;
      if (keyClass != NullWritable.class) {
        key = (Writable) ReflectionUtils.newInstance(keyClass, getConf());
        key.readFields(in);
      }

      Class valueClass = getClass(in.readByte());
      Writable value = null;
      if (valueClass != NullWritable.class) {
        value = (Writable) ReflectionUtils.newInstance(valueClass, getConf());
        value.readFields(in);
      }

      instance.put(key, value);
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer("[");
    for (Map.Entry<Writable, Writable> entry : entrySet()) {
      sb.append("{");
      sb.append(entry.getKey().toString());
      sb.append(":");
      sb.append(entry.getValue().toString());
      sb.append("}");
    }
    sb.append("]");
    return sb.toString();
  }
}
