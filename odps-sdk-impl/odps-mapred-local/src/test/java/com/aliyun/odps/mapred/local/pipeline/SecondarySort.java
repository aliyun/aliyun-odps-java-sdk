/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mapred.local.pipeline;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.Builder;

/**
 * This is an example ODPS Map/Reduce application. It reads the input table that
 * must contain two integers per record. The output is sorted by the first and
 * second number and grouped on the first number.
 *
 * To run: jar -libjars mapreduce-examples.jar -classpath
 * clt/lib/mapreduce-examples.jar
 * com.aliyun.odps.mapreduce.examples.SecondarySort <i>in-table</i>
 * <i>out-table</i>
 */
public class SecondarySort extends PipelineExampleBase {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

  /**
   * Read two integers from each line and generate a key, value pair as ((left,
   * right), right).
   */
  public static class MapClass extends MapperBase {

    private Record key;
    private Record value;

    @Override
    public void setup(TaskContext context) throws IOException {
      key = context.createMapOutputKeyRecord();
      value = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      long left = 0;
      long right = 0;

      if (record.getColumnCount() > 0) {
        left = (Long) record.get(0);
        if (record.getColumnCount() > 1) {
          right = (Long) record.get(1);
        }

        key.set(new Object[]{(Long) left, (Long) right});
        value.set(new Object[]{(Long) right});
        // key.setBigint(0, left);
        // key.setBigint(1, right);
        // value.setBigint(0, right);

        context.write(key, value);
      }
    }
  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class ReduceClass extends ReducerBase {

    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      result.set(0, key.get(0));
      while (values.hasNext()) {
        Record value = values.next();
        result.set(1, value.get(0));
        context.write(result);
      }
    }

  }

  /**
   * A reducer class that just emits the sum of the input values.
   */
  public static class MedialReduceClass extends ReducerBase {

    private Record val = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      val = context.createOutputValueRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      while (values.hasNext()) {
        Record value = values.next();
        val.set(0, value.get(0));
        context.write(key, value);
      }
    }

  }

  public static void setMrMR(JobConf job, Builder builder) {
    // mapper
    builder
        .addMapper(MapClass.class)
        .setOutputKeySchema(
            new Column[]{new Column("key", OdpsType.BIGINT), new Column("value", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"key", "value"})
        .setPartitionColumns(new String[]{"key"})
        .setOutputGroupingColumns(new String[]{"key"});

    // reducer
    builder.addReducer(ReduceClass.class)
        .setOutputKeySchema(new Column[]{new Column("key", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("vaule", OdpsType.BIGINT)});
  }

  public static void setMrMRR(JobConf job, Builder builder) {
    // mapper
    builder
        .addMapper(MapClass.class)
        .setOutputKeySchema(
            new Column[]{new Column("key", OdpsType.BIGINT), new Column("value", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"key", "value"})
        .setPartitionColumns(new String[]{"key"})
        .setOutputGroupingColumns(new String[]{"key"});

    // reducer
    builder
        .addReducer(MedialReduceClass.class)
        .setOutputKeySchema(
            new Column[]{new Column("key", OdpsType.BIGINT), new Column("value", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"key", "value"})
        .setPartitionColumns(new String[]{"key", "value"})
        .setOutputGroupingColumns(new String[]{"key", "value"});

    // reducer
    builder.addReducer(ReduceClass.class)
        .setOutputKeySchema(new Column[]{new Column("key", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("vaule", OdpsType.BIGINT)});
  }

  public static void setMrMRRR(JobConf job, Builder builder) {
    // mapper
    builder
        .addMapper(MapClass.class)
        .setOutputKeySchema(
            new Column[]{new Column("key", OdpsType.BIGINT), new Column("value", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"key"})
        .setPartitionColumns(new String[]{"key"})
        .setOutputGroupingColumns(new String[]{"key"});

    // reducer
    builder
        .addReducer(MedialReduceClass.class)
        .setOutputKeySchema(
            new Column[]{new Column("key", OdpsType.BIGINT), new Column("value", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"key", "value"})
        .setPartitionColumns(new String[]{"key"})
        .setOutputGroupingColumns(new String[]{"key"});

    // reducer
    builder
        .addReducer(MedialReduceClass.class)
        .setOutputKeySchema(
            new Column[]{new Column("key", OdpsType.BIGINT), new Column("value", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"key", "value"})
        .setPartitionColumns(new String[]{"key", "value"})
        .setOutputGroupingColumns(new String[]{"key", "value"});

    // reducer
    builder.addReducer(ReduceClass.class)
        .setOutputKeySchema(new Column[]{new Column("key", OdpsType.BIGINT)})
        .setOutputValueSchema(new Column[]{new Column("vaule", OdpsType.BIGINT)});
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[2];
    args[0] = "pipe_secondarysort_in";
    args[1] = "pipe_secondarysort_out";

    JobConf job = new JobConf();
    Builder builder = Pipeline.builder();

    if (args.length == 2) {
      setMrMR(job, builder);
    } else if (args.length == 3) {
      String pattern = args[2];

      if (pattern.equalsIgnoreCase("M-R")) {
        setMrMR(job, builder);
      } else if (pattern.equalsIgnoreCase("M-R-R")) {
        setMrMRR(job, builder);
      } else if (pattern.equalsIgnoreCase("M-R-R-R")) {
        setMrMRRR(job, builder);
      }
    }

    Pipeline pipeline = builder.createPipeline();

    addInputTable(job, args[0]);
    addOutputTable(job, args[1]);

    JobClient.runJob(job, pipeline);
  }

}
