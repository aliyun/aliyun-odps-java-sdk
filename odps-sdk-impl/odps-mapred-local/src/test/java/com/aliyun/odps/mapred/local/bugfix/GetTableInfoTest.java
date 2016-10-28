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

package com.aliyun.odps.mapred.local.bugfix;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

/**
 * This is an example ODPS Map/Reduce application. It reads the input table, map
 * each column into words and counts them. The output is a locally sorted list
 * of words and the count of how often they occurred.
 * <p>
 * To run: jar -libjars mapreduce-examples.jar -classpath
 * clt/lib/mapreduce-examples.jar com.aliyun.odps.mapreduce.examples.WordCount
 * <i>in-tbl</i> <i>out-tbl</i>
 */
public class GetTableInfoTest {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  /**
   * Counts the words in each record. For each record, emit each column as
   * (<b>word</b>, <b>1</b>).
   */
  public static class TokenizerMapper extends MapperBase {

    Counter gCnt;
    Record result;

    @Override
    public void setup(TaskContext context) throws IOException {

      help(context, "mapSetup");

      gCnt = context.getCounter("MyCounters", "global_counts");
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      help(context, "map");
    }

    public void help(TaskContext context, String stage) throws IOException {
      // columns=project:STRING,table:STRING,type:STRING,colNum:BIGINT,cols:STRING,stage:STRING

      Record result = context.createOutputRecord();
      TableInfo info = context.getInputTableInfo();
      result.set(0, info.toString());
      result.set(1, "in");
      String[] cols = info.getCols();
      result.set(2, cols.length);

      String colNames = "";
      for (int i = 0; i < cols.length; i++) {
        if (colNames.length() > 0) {
          colNames += ",";
        }
        colNames += cols[i];
      }

      result.set(3, colNames);
      result.set(4, stage);

      context.write(result);

      TableInfo[] outputTableInfos = context.getOutputTableInfo();
      for (int i = 0; i < outputTableInfos.length; i++) {
        info = outputTableInfos[i];

        result.set(0, info.toString());
        result.set(1, "out");
        cols = info.getCols();
        result.set(2, cols.length);

        colNames = "";
        for (i = 0; i < cols.length; i++) {
          if (colNames.length() > 0) {
            colNames += ",";
          }
          colNames += cols[i];
        }

        result.set(3, colNames);
        result.set(4, stage);

        context.write(result);
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

    @Override
    public void setup(TaskContext context) throws IOException {

    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {

    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[4];
    args[0] = "l_ss";
    args[1] = "l_p_ss";
    args[2] = "getTableInfo_out";

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(SumReducer.class);
    job.setNumReduceTasks(0);

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).partSpec("p1=1/p2=2").build(), job);
    InputUtils.addTable(TableInfo.builder().tableName(args[1]).partSpec("p1=1").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(), job);

    JobClient.runJob(job);
  }

}
