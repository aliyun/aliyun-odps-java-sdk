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

package com.aliyun.odps.mapred;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;

/**
 * Reducer 的空实现，详情请参见 {@link Reducer}
 */
public class ReducerBase implements Reducer {

  @Override
  public void setup(TaskContext context) throws IOException {

  }

  @Override
  public void reduce(Record key, Iterator<Record> values, TaskContext context)
      throws IOException {

  }


  @Override
  public void cleanup(TaskContext context) throws IOException {

  }

  public void run(TaskContext context) throws IOException {
    setup(context);
    while (context.nextKeyValue()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
    }
    cleanup(context);
  }

  /**
   * @Deprecated Use {@link #setup(com.aliyun.odps.mapred.Reducer.TaskContext)} instead.
   */
  @Deprecated
  public void setup(com.aliyun.odps.mapred.TaskContext context) throws IOException {
  }

  /**
   * @Deprecated Use {@link #reduce(Record, Iterator, com.aliyun.odps.mapred.Reducer.TaskContext)}
   * instead.
   */
  @Deprecated
  public void reduce(Record key, Iterator<Record> values,
                     com.aliyun.odps.mapred.TaskContext context)
      throws IOException {

  }

  /**
   * @Deprecated Use {@link #cleanup(com.aliyun.odps.mapred.Reducer.TaskContext)} instead.
   */
  @Deprecated
  public void cleanup(com.aliyun.odps.mapred.TaskContext context) throws IOException {

  }

}
