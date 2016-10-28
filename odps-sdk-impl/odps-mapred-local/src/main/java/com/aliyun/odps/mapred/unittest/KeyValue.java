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

package com.aliyun.odps.mapred.unittest;

/**
 * 键值对（Key/Value）.
 * 
 * @param <K>
 *          Key 类型
 * @param <V>
 *          Value 类型
 */
public class KeyValue<K, V> {

  private K k;
  private V v;

  public KeyValue(K k, V v) {
    this.k = k;
    this.v = v;
  }

  public V getValue() {
    return v;
  }

  public K getKey() {
    return k;
  }

  public void setKey(K k) {
    this.k = k;
  }

  public void setValue(V v) {
    this.v = v;
  }

  @Override
  public String toString() {
    return "KeyValue: " + k + ", " + v;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof KeyValue<?, ?>) {
      KeyValue<K, V> that = (KeyValue<K, V>) obj;
      return k.equals(that.k) && v.equals(that.v);
    }
    return false;
  }

}
