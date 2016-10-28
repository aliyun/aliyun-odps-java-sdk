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

package com.aliyun.odps.mapred.local.lib;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

/**
 * IdentityReducer
 *
 * @author mingdi
 */
public class IdentityReducer extends ReducerBase {

  private Record result = null;

  @Override
  public void setup(TaskContext context) throws IOException {
    result = context.createOutputRecord();
  }

  /**
   * Writes all keys and values directly to output.
   */
  @Override
  public void reduce(Record key, Iterator<Record> values, TaskContext context)
      throws IOException {
    result.set(0, key.get(0));

    while (values.hasNext()) {
      Record val = values.next();
      result.set(1, val.get(0));
      context.write(result);
    }
  }

}

// public class IdentityReducer<KEYIN, VALUEIN> extends Reducer<KEYIN, VALUEIN>
// {
// private Record result = null;
//
// @Override
// protected void setup(ReduceContext<KEYIN, VALUEIN> context)
// throws IOException, InterruptedException {
// result = context.createOutputRecord();
// }
//
// /** Writes all keys and values directly to output. */
// @Override
// public void reduce(KEYIN key, Iterable<VALUEIN> values,
// ReduceContext<KEYIN, VALUEIN> context) throws IOException,
// InterruptedException {
//
// result.set(0, (WritableComparable) key);
// for (VALUEIN v : values) {
// result.set(1, (Writable) v);
// context.write(result);
// }
// }
// }
