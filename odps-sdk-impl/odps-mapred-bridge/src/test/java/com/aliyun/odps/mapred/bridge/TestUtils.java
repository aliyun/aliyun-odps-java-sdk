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

package com.aliyun.odps.mapred.bridge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.FieldSchema;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Writable;

import difflib.Delta;
import difflib.DiffUtils;
import difflib.Patch;

public class TestUtils {

  private static final Log LOG = LogFactory.getLog(TestUtils.class);

  private static List<String> fileToLines(String path) {
    List<String> lines = new LinkedList<String>();
    String line = "";
    try {
      BufferedReader in = new BufferedReader(new FileReader(path));
      while ((line = in.readLine()) != null) {
        lines.add(line);
      }
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return lines;
  }

  public static boolean verifySql(String expectedFileName, String sql) {
    List<String> original = fileToLines("data/" + expectedFileName);
    List<String> revised = Arrays.asList(sql.split("\n"));

    LOG.info(sql);
    // Compute diff. Get the Patch object. Patch is the container for computed
    // deltas.
    Patch patch = DiffUtils.diff(original, revised);

    if (patch.getDeltas().isEmpty()) {
      return true;
    } else {
      for (Delta delta : patch.getDeltas()) {
        System.out.println(delta.toString());
      }
      return false;
    }
  }

  public static com.aliyun.odps.Record mockOldRecord(OdpsType[] types, String[] names,
                                                     final Writable[] values) {
    assert types.length == names.length;

    final FieldSchema[] schema = new FieldSchema[types.length];
    final Map<String, Integer> name2Idx = new HashMap<String, Integer>();
    for (int i = 0; i < names.length; i++) {
      name2Idx.put(names[i], i);
      schema[i] = new FieldSchema(names[i], types[i].toString());
    }

    return new com.aliyun.odps.Record() {

      @Override
      public int size() {
        return schema.length;
      }

      @Override
      public boolean isNull(int idx) {
        return values[idx] == null || values[idx] instanceof NullWritable;
      }

      @Override
      public boolean isNull(String fieldName) throws IOException {
        return isNull(name2Idx.get(fieldName));
      }

      @Override
      public Writable get(int index) {
        return values[index];
      }

      @Override
      public Writable get(String fieldName) throws IOException {
        return get(name2Idx.get(fieldName));
      }

      @Override
      public void set(int index, Writable value) {
        values[index] = value;
      }

      @Override
      public void set(String fieldName, Writable value) throws IOException {
        set(name2Idx.get(fieldName), value);
      }

      @Override
      public void set(Writable[] values) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public FieldSchema getField(int index) {
        return schema[index];
      }

      @Override
      public FieldSchema[] getFields() {
        return schema;
      }

      @Override
      public Writable[] getAll() {
        return values;
      }

      @Override
      public String toDelimitedString() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String toDelimitedString(char delim, String nullIndicator) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fromDelimitedString(String delimStr) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public void fromDelimitedString(String delimStr, char delim, String nullIndicator)
          throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public com.aliyun.odps.Record clone() {
        return null;
      }
    };
  }
}
