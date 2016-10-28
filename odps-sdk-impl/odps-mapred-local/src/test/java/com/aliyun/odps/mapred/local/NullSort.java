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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

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
 * This is the trivial map/reduce program that does absolutely nothing other
 * than use the framework to fragment and sort the input values.
 * <p>
 * To run: jar -libjars mapreduce-examples.jar -classpath
 * clt/lib/mapreduce-examples.jar com.aliyun.odps.mapreduce.examples.Sort [-m
 * <i>maps</i>] [-mapoutKey <i>output key class</i>] [-mapoutValue <i>output
 * value class</i>] <i>in-tbl</i> <i>out-tbl</i>
 */
public class NullSort {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  static int printUsage() {
    System.out.println("sort [-m <maps>] [-r <reduces>] "
                       + "[-inFormat <input format class>] "
                       + "[-outFormat <output format class>] "
                       + "[-outKey <output key class>] " + "[-outValue <output value class>] "
                       + "[-totalOrder <pcnt> <num samples> <max splits>] "
                       + "<input> <output>");
    return -1;
  }

  /**
   * Implements the identity function, mapping record's first two columns to
   * outputs.
   */
  public static class IdentityMapper extends MapperBase {

    private Record word;
    private Record one;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context)
        throws IOException {
      word.set(new Object[]{(Long) record.get(0)});
      one.set(new Object[]{(Long) record.get(1)});
      context.write(word, one);
    }

  }

  public static class IdentityReducer extends ReducerBase {

    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    /**
     * Writes all keys and values directly to output.
     */
    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {
      result.set(0, key.get(0));

      while (values.hasNext()) {
        Record val = values.next();
        result.set(1, val.get(0));
        context.write(result);
      }
    }

  }

  /**
   * The main driver for sort program. Invoke this method to submit the
   * map/reduce job.
   *
   * @throws IOException
   *     When there is communication problems with the job tracker.
   */
  @Test
  public void test() throws Exception {
    String[] args = new String[2];
    args[0] = "nullsort_in";
    args[1] = "nullsort_out";

    JobConf jobConf = new JobConf();

    jobConf.setMapperClass(IdentityMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);

    // Class<? extends WritableComparable> outputKeyClass = BytesWritable.class;
    // Class<? extends Writable> outputValueClass = BytesWritable.class;
    List<String> otherArgs = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-mapoutKey".equals(args[i])) {
          // outputKeyClass = Class.forName(args[++i]).asSubclass(
          // WritableComparable.class);
          ++i;
        } else if ("-mapoutValue".equals(args[i])) {
          // outputValueClass = Class.forName(args[++i])
          // .asSubclass(Writable.class);
          ++i;
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
                           + args[i - 1]);
        printUsage(); // exits
      }
    }

    // Set user-supplied (possibly default) job configs
    jobConf.setNumReduceTasks(1);

    // jobConf.setMapOutputKeyClass(outputKeyClass);
    // jobConf.setMapOutputValueClass(outputValueClass);
    jobConf.setMapOutputKeySchema(SchemaUtils.fromString("key:bigint"));
    jobConf.setMapOutputValueSchema(SchemaUtils.fromString("value:bigint"));

    // Make sure there are exactly 2 parameters left.
    if (otherArgs.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: "
                         + otherArgs.size() + " instead of 2.");
      printUsage();
    }

    InputUtils.addTable(TableInfo.builder().tableName(args[0])
                            .build(), jobConf);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
                         jobConf);

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(jobConf);
    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    System.out.println("The job took "
                       + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");
  }
}
