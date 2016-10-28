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

package com.aliyun.odps.mapred.local.pipeline;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

/**
 * Base Class of all PipeLine Test
 *
 * @author yubin.byb
 */
public abstract class PipeLine {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

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
      one.setBigint(0, 1L);
      gCnt = context.getCounter("MyCounters", "global_counts");
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
        String[] words = record.get(i).toString().split("\\s+");
        for (String w : words) {
          word.setString(0, w);
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
  public static class SumIReducer extends ReducerBase {

    private Record word;
    private Record sum;
    Counter gCnt;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createOutputKeyRecord();
      sum = context.createOutputValueRecord();
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

      word.set(0, key.get(0));
      word.setBigint(1, count);
      sum.setBigint(0, count);

      Counter cnt = context.getCounter("MyCounters", "reduce_outputs");
      cnt.increment(1);
      gCnt.increment(1);

      context.write(word, sum);
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

        Counter cnt = context.getCounter("MyCounters", "reduce_outputs");
        cnt.increment(1);
        gCnt.increment(1);
      }

      result.set(0, key.get(0));
      result.set(1, count);
      context.write(result);
    }
  }

  /**
   * IdentityMapper
   */
  public static class IdentityMapper extends MapperBase {

    Record v;

    @Override
    public void setup(TaskContext context) throws IOException {
      v = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      context.write(record, v);

      Counter cnt = context.getCounter("MapCounters", "map_input_records");
      cnt.increment(1);
    }
  }

  /**
   * Identity Reducer Medial
   */
  public static class IdentityIReducer extends ReducerBase {

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        Record val = values.next();
        context.write(key, val);

        Counter cnt = context.getCounter("ReduceCounters",
                                         "reduce_input_records");
        cnt.increment(1);
      }
    }
  }

  /**
   * IdentityReducer
   */
  public static class IdentityReducer extends ReducerBase {

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        @SuppressWarnings("unused")
        Record val = values.next();
        context.write(key);

        Counter cnt = context.getCounter("ReduceCounters",
                                         "reduce_input_records");
        cnt.increment(1);
      }
    }
  }

  /**
   * mr_src, project.mr_src, mr_srcpart|pt=1, project.mr_srcpart|pt=2/ds=2
   *
   * @param job
   * @param input
   * @throws Exception
   */
  public static void addInputTable(JobConf job, final String input)
      throws Exception {
    String[] table = input.trim().split("\\|");

    if (table.length == 1) { // project.mr_src
      String[] ss = table[0].trim().split("\\.");
      if (ss.length == 1) {
        InputUtils.addTable(TableInfo.builder().tableName(ss[0]).build(), job);
      } else if (ss.length == 2) {
        InputUtils.addTable(
            TableInfo.builder().projectName(ss[0]).tableName(ss[1]).build(),
            job);
      } else {
        throw new Exception("Style of input " + input + " wrong");
      }
    } else if (table.length == 2) { // project.mr_srcpart|pt=2/ds=2
      String[] ss = table[0].trim().split("\\.");
      if (ss.length == 1) {
        InputUtils.addTable(
            TableInfo.builder().tableName(ss[0]).partSpec(table[1]).build(),
            job);
      } else if (ss.length == 2) {
        InputUtils.addTable(
            TableInfo.builder().projectName(ss[0]).tableName(ss[1])
                .partSpec(table[1]).build(), job);
      } else {
        throw new Exception("Style of input " + input + " wrong");
      }
    } else {
      throw new Exception("Style of input " + input + " wrong");
    }
  }

  /**
   * mr_multiinout_out1, project.mr_multiinout_out1, mr_multiinout_out2|a=1/b=1,
   * project.mr_multiinout_out2|a=2/b=2|out2;
   *
   * @param job
   * @param table
   * @throws Exception
   */
  public static void addOutputTable(JobConf job, final String output)
      throws Exception {
    String[] table = output.split("\\|");

    if (table.length == 1) { // project.mr_multiinout_out1
      String[] ss = table[0].trim().split("\\.");
      if (ss.length == 1) {
        OutputUtils.addTable(TableInfo.builder().tableName(ss[0]).build(), job);
      } else if (ss.length == 2) {
        OutputUtils.addTable(
            TableInfo.builder().projectName(ss[0]).tableName(ss[1]).build(),
            job);
      } else {
        throw new Exception("Style of output " + output + " wrong");
      }
    } else if (table.length == 2) { // project.mr_multiinout_out2|a=1/b=1
      String[] ss = table[0].trim().split("\\.");
      if (ss.length == 1) {
        OutputUtils.addTable(
            TableInfo.builder().tableName(ss[0]).partSpec(table[1]).build(),
            job);
      } else if (ss.length == 2) {
        OutputUtils.addTable(
            TableInfo.builder().projectName(ss[0]).tableName(ss[1])
                .partSpec(table[1]).build(), job);
      } else {
        throw new Exception("Style of output " + output + " wrong");
      }
    } else if (table.length == 3) { // project.mr_multiinout_out2|a=2/b=2|out2
      String[] ss = table[0].trim().split("\\.");
      if (ss.length == 1) {
        OutputUtils.addTable(
            TableInfo.builder().tableName(ss[0]).partSpec(table[1])
                .label(table[2]).build(), job);
      } else if (ss.length == 2) {
        OutputUtils.addTable(
            TableInfo.builder().projectName(ss[0]).tableName(ss[1])
                .partSpec(table[1]).label(table[2]).build(), job);
      } else {
        throw new Exception("Style of output " + output + " wrong");
      }
    } else {
      throw new Exception("Style of output " + output + " wrong");
    }
  }

}
