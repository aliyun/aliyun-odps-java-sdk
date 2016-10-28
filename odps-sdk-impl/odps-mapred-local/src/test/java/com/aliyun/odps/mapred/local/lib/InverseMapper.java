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

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.MapperBase;

/**
 * A {@link Mapper} that swaps keys and values.
 *
 * @author mingdi
 */
public class InverseMapper extends MapperBase {

  private Record word;
  private Record count;

  @Override
  public void setup(TaskContext context) throws IOException {
    word = context.createMapOutputValueRecord();
    count = context.createMapOutputKeyRecord();
  }

  /**
   * The inverse function. Input keys and values are swapped.
   */
  @Override
  public void map(long recordNum, Record record, TaskContext context)
      throws IOException {
    word.set(new Object[]{record.get(0).toString()});
    count.set(new Object[]{record.get(1)});
    context.write(count, word);
  }

}

// public class InverseMapper extends Mapper<LongWritable, Text> {
//
// /** The inverse function. Input keys and values are swapped. */
// public void map(LongWritable key, Record value,
// MapContext<LongWritable, Text> context) throws IOException,
// InterruptedException {
// context.write((LongWritable) value.get(1), (Text) value.get(0));
// }
// }
