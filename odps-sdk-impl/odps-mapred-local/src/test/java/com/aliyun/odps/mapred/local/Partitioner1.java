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

package com.aliyun.odps.mapred.local;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Partitioner;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

class MyPartitioner1 extends Partitioner {

  static int i = 0;

  @Override
  public int getPartition(Record key, Record value, int numPartitions) {
    i++;
    return i % 3;
  }

}

public class Partitioner1 {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  public static class TokenizerMapper extends MapperBase {

    Record word;
    Record one;
    Counter gCnt;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.set(new Object[]{1L});
      gCnt = context.getCounter("MyCounters", "global_counts");
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
        String[] words = record.get(i).toString().split("\\s+");
        for (String w : words) {
          word.set(new Object[]{w});
          Counter cnt = context.getCounter("MyCounters", "map_outputs");
          cnt.increment(1);
          gCnt.increment(1);
          context.write(word, one);
        }
      }
    }
  }

  /**
   * A combiner class that combines map output by sum them.
   */
  public static class SumCombiner extends ReducerBase {

    private Record count;

    @Override
    public void setup(TaskContext context) throws IOException {
      count = context.createMapOutputValueRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      long c = 0;
      while (values.hasNext()) {
        Record val = values.next();
        c += (Long) val.get(0);
      }
      count.set(0, c);
      context.write(key, count);
    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class SumReducer extends ReducerBase {

    private Record result;
    Counter gCnt;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
      gCnt = context.getCounter("MyCounters", "global_counts");
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
      Counter cnt = context.getCounter("MyCounters", "reduce_outputs");
      cnt.increment(1);
      gCnt.increment(1);

      context.write(result);
    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[3];
    args[0] = "l_ss";
    args[2] = "partition1_out";

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setNumReduceTasks(3);
    job.setPartitionerClass(MyPartitioner1.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }

}
