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

package com.aliyun.odps;

import java.io.IOException;

import com.aliyun.odps.io.BooleanWritable;
import com.aliyun.odps.io.DatetimeWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.InputBlobWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.OutputBlobWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;

@Deprecated
public interface Record {

  public final static char DELIM = '\u0001';

  public final static String NULLINDICATOR = "\\N";

  public int size();

  public boolean isNull(int idx);

  public boolean isNull(String fieldName) throws IOException;

  public Writable get(int index);

  public Writable get(String fieldName) throws IOException;

  public void set(int index, Writable value);

  public void set(String fieldName, Writable value) throws IOException;

  public void set(Writable[] values) throws IOException;

  public FieldSchema getField(int index);

  public FieldSchema[] getFields();

  public Writable[] getAll();

  public String toDelimitedString();

  public String toDelimitedString(char delim, String nullIndicator);

  public void fromDelimitedString(String delimStr) throws IOException;

  public void fromDelimitedString(String delimStr, char delim,
                                  String nullIndicator) throws IOException;

  public Record clone();
}
