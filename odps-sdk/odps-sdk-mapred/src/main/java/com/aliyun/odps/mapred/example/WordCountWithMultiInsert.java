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

package com.aliyun.odps.mapred.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.example.WordCount.TokenizerMapper;
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
public class WordCountWithMultiInsert {

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class SumReducer extends ReducerBase {

    private Map<String, Record> result;

    @Override
    public void setup(TaskContext context) throws IOException {
      TableInfo[] tbls = context.getOutputTableInfo();
      result = new HashMap<String, Record>();
      for (TableInfo tbl : tbls) {
        String lable = tbl.getLabel();
        result.put(lable, context.createOutputRecord(lable));
      }
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      long count = 0;
      while (values.hasNext()) {
        Record val = values.next();
        count += (Long) val.get(0);
      }

      for (String label : result.keySet()) {
        Record r = result.get(label);
        r.set(0, key.get(0));
        r.set(1, count);
        context.write(r, label);
      }
    }
  }

  public static void main(String[] args) throws OdpsException {
    if (args.length != 2) {
      System.err.println("Usage: wordcount <in_table> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumReducer.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)});
    job.setMapOutputValueSchema(new Column[]{new Column("count",
                                                        OdpsType.BIGINT)});

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    for (int i = 1; i < args.length; i++) {
      OutputUtils.addTable(
          TableInfo.builder().tableName(args[i])
              .label(String.format("out%d", i)).build(), job);
    }

    JobClient.runJob(job);
  }

}
