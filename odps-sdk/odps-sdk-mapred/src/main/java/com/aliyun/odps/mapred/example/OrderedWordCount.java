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
import java.util.Iterator;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Job;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.pipeline.Pipeline;

public class OrderedWordCount {

  /**
   * Counts the words in each record. For each record, emit each column as
   * (<b>word</b>, <b>1</b>).
   */
  public static class TokenizerMapper extends MapperBase {

    Record word;
    Record one;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.setBigint(0, 1L);
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
        String[] words = record.get(i).toString().split("\\s+");
        for (String w : words) {
          word.setString(0, w);
          context.write(word, one);
        }
      }
    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class SumReducer extends ReducerBase {

    private Record num;
    private Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
      num = context.createOutputKeyRecord();
      result = context.createOutputValueRecord();
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
      num.set(0, count);
      context.write(num, result);
    }
  }

  /**
   * A reducer class that just writes all values directly to output
   */
  public static class IdentityReducer extends ReducerBase {

    /**
     * Writes all values directly to output.
     */
    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        context.write(values.next());
      }
    }
  }

  public static void main(String[] args) throws OdpsException {
    if (args.length != 2) {
      System.err.println("Usage: orderedwordcount <in_table> <out_table>");
      System.exit(2);
    }

    Job job = new Job();

    Pipeline pipeline = Pipeline.builder()
        .addMapper(TokenizerMapper.class)
        .setOutputKeySchema(
            new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(
            new Column[]{new Column("count", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"word"})
        .setPartitionColumns(new String[]{"word"})
        .setOutputGroupingColumns(new String[]{"word"})

        .addReducer(SumReducer.class)
        .setOutputKeySchema(
            new Column[]{new Column("count", OdpsType.BIGINT)})
        .setOutputValueSchema(
            new Column[]{new Column("word", OdpsType.STRING),
                         new Column("count", OdpsType.BIGINT)})

        .addReducer(IdentityReducer.class).createPipeline();

    job.setPipeline(pipeline);

    job.addInput(TableInfo.builder().tableName(args[0]).build());
    job.addOutput(TableInfo.builder().tableName(args[1]).build());

    job.submit();
    job.waitForCompletion();
    System.exit(job.isSuccessful() == true ? 0 : 1);
  }

}
