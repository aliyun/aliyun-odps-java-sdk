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

package com.aliyun.odps.graph.local;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.graph.local.utils.LocalGraphRunUtils;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.utils.ReflectionUtils;


public class SQLRecord implements WritableRecord {

  private Column[] cols;
  private Map<String, Integer> mapIdx;
  private byte[] types;
  private Writable[] fields;

  public SQLRecord(Column[] columns, byte[] types) {
    if (columns == null) {
      throw new IllegalArgumentException();
    }
    this.cols = columns;
    this.types = types;
    this.mapIdx = new HashMap<String, Integer>();
    fields = new Writable[columns.length];
    for (int i = 0; i < columns.length; i++) {
      mapIdx.put(columns[i].getName(), i);
    }
  }

  public SQLRecord(Column[] columns) {
    this(columns, null);
  }

  @Override
  public int size() {
    return cols.length;
  }

  @Override
  public boolean isNull(int idx) {
    return fields[idx] == null || fields[idx] instanceof NullWritable;
  }

  @Override
  public boolean isNull(String fieldName) throws IOException {
    Integer idx = mapIdx.get(fieldName);
    if (idx == null) {
      throw new IOException("Invalid field name: " + fieldName);
    }
    return isNull(idx);
  }

  @Override
  public Writable get(int idx) {
    return fields[idx];
  }

  @Override
  public Writable get(String fieldName) throws IOException {
    Integer idx = mapIdx.get(fieldName);
    if (idx == null) {
      throw new IOException("Invalid field name: " + fieldName);
    }
    return fields[idx];
  }

  @Override
  public void set(int idx, Writable value) {
    checkColIndex(idx);
    fields[idx] = value;
  }

  @Override
  public void set(String fieldName, Writable value) throws IOException {
    Integer idx = mapIdx.get(fieldName);
    if (idx == null) {
      throw new IOException("Invalid field name: " + fieldName);
    }
    set(idx, value);
  }

  @Override
  public void set(Writable[] values) throws IOException {
    if (cols.length != values.length) {
      throw new IOException("Size unmatch: " + values.length + ", expect: "
                            + cols.length);
    }
    for (int idx = 0; idx < cols.length; idx++) {
      this.fields[idx] = values[idx];
    }

  }

  @Override
  public Column getField(int index) {
    return cols[index];
  }

  @Override
  public Column[] getFields() {
    return cols;
  }

  @Override
  public Writable[] getAll() {
    return fields;
  }

  @Override
  public String toString() {
    return "SQLRecord [cols=" + Arrays.toString(cols) + ", fields="
           + Arrays.toString(fields) + "]";
  }

  Writable[] getInternalValues() {
    return fields;
  }

  private void checkColIndex(int index) {
    if (index >= size() || index < 0) {
      throw new RuntimeException(
          "ODPS-0730001: Column index out of bound - col index '" + index
          + "' must be in range [0, " + (size() - 1) + "]");
    }
  }


  @Override
  public SQLRecord clone() {
    SQLRecord rec = new SQLRecord(cols, types);
    rec.fields = LocalGraphRunUtils.createFileds(types);
    for (int i = 0; i < fields.length; i++) {
      if (fields[i] != null) {
        if (fields[i] instanceof NullWritable) {
          rec.fields[i] = NullWritable.get();
        } else {
          try {
            ReflectionUtils.cloneWritableInto(rec.fields[i], fields[i]);
          } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
          }
        }
      } else {
        rec.fields[i] = null;
      }
    }
    return rec;
  }
}
