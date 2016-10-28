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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * This is an example ODPS Map/Reduce application. It reads the input table, map
 * each column into words and counts them. The output is a locally sorted list
 * of words and the count of how often they occurred.
 * <p>
 * To run: jar -libjars mapreduce-examples.jar -classpath
 * clt/lib/mapreduce-examples.jar com.aliyun.odps.mapreduce.examples.WordCount
 * <i>in-tbl</i> <i>out-tbl</i>
 */
public class WordCount {

  /**
   * Counts the words in each record. For each record, emit each column as
   * (<b>word</b>, <b>1</b>).
   */
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
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
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

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: wordcount <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).partSpec("p1=1/p2=2").build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).partSpec("p1=1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }

  @Test
  public void test() throws Exception {
    WareHouse wareHouse = WareHouse.getInstance();
    String project = TestUtils.odps_test_mrtask;
    String outputTable = "wordcount_out";

    TestUtils.setEnvironment(project);

    //If output table exists then delete data (will not delete schema)
    wareHouse.dropTableDataIfExists(project, outputTable, null);
    Assert.assertEquals(true, wareHouse.isTableEmpty(project, outputTable, null));

    new WordCount().main(new String[]{"l_ss", "l_p_ss", outputTable});

    Assert.assertEquals(false, wareHouse.isTableEmpty(project, outputTable, null));

    //read output table data
    List<Object[]> result = wareHouse.readData(project, outputTable, null, null, ',');
    Assert.assertEquals(4, result.size());

    // Sampling inspection
    Object[] record = result.get(0);
    Assert.assertEquals(2, record.length);
    Assert.assertEquals(true, record[0] instanceof String);
    Assert.assertEquals(true, record[1] instanceof Long);
    Assert.assertEquals("key1", record[0]);
    Assert.assertEquals(4L, record[1]);

  }


}
