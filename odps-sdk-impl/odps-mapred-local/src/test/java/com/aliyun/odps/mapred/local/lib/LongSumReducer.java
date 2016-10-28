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
 * LongSumReducer
 *
 * @author mingdi
 */
public class LongSumReducer extends ReducerBase {

  private Record result = null;

  @Override
  public void setup(TaskContext context) throws IOException {
    result = context.createOutputRecord();
  }

  @Override
  public void reduce(Record key, Iterator<Record> values, TaskContext context)
      throws IOException {
    long count = 0;
    while (values.hasNext()) {
      Record val = values.next();
      count += (Long) val.get(0);
    }
    result.set(0, key.get(0));
    result.set(1, count);
    context.write(result);
  }

}

/*
 * public class LongSumReducer<KEYIN> extends Reducer<KEYIN, LongWritable> {
 * private LongWritable sum = new LongWritable(); private Record result = null;
 * 
 * @Override protected void setup(ReduceContext<KEYIN, LongWritable> context)
 * throws IOException, InterruptedException { result =
 * context.createOutputRecord(); }
 * 
 * @Override public void reduce(KEYIN key, Iterable<LongWritable> values,
 * ReduceContext<KEYIN, LongWritable> context) throws IOException,
 * InterruptedException {
 * 
 * // sum all values for this key long count = 0; for (LongWritable lw : values)
 * { count += lw.get(); }
 * 
 * // output sum sum.set(count); result.set(0, (Writable) key); result.set(1,
 * sum); context.write(result); } }
 */
