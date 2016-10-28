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

package com.aliyun.odps.mapred.local;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

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
public class SecondarySort {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_openmr2);
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

  @Test
  public void test() throws OdpsException {
    String[] args = new String[2];
    args[0] = "mr_sort_in";
    args[1] = "mr_secondarysort_out";

    JobConf job = new JobConf();
    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);

    // compare first and second parts of the pair
    job.setOutputKeySortColumns(new String[]{"i1", "i2"});

    // partition based on the first part of the pair
    job.setPartitionColumns(new String[]{"i1"});

    // grouping comparator based on the first part of the pair
    job.setOutputGroupingColumns(new String[]{"i1"});

    // the map output is LongPair, Long
    job.setMapOutputKeySchema(SchemaUtils.fromString("i1:bigint,i2:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("i2x:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
