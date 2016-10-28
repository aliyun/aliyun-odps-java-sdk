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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.mapred.unittest.*;

public class BinaryWordCountTest extends MRUnitTest {
  private final static String INPUT_SCHEMA = "a:string,b:string";
  private final static String OUTPUT_SCHEMA = "k:string,v:bigint";
  private JobConf job;

  public static class TokenizerMapper extends MapperBase {
    private Record word;
    private Record one;

    @Override
    public void setup(TaskContext context) throws IOException {
      word = context.createMapOutputKeyRecord();
      one = context.createMapOutputValueRecord();
      one.set(new Object[] {1L});
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
      for (int i = 0; i < record.getColumnCount(); i++) {
          word.set(new Object[] {record.getBytes(i)});
          context.write(word, one);
      }
    }
  }

  /**
   * A combiner class that combines map output by sum them.
   **/
  public static class SumCombiner extends ReducerBase {
    private Record count;

    @Override
    public void setup(TaskContext context) throws IOException {
      count = context.createMapOutputValueRecord();
      
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
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
   **/
  public static class SumReducer extends ReducerBase {
    private Record result = null;

    @Override
    public void setup(TaskContext context) throws IOException {
      result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
      long count = 0;
      while (values.hasNext()) {
        Record val = values.next();
        count += (Long) val.get(0);
      }
      result.set(0, key.getBytes(0));
      result.set(1, count);
      context.write(result);
    }
  }

  public BinaryWordCountTest() throws Exception {
    job = new JobConf();

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("value:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName("wc_in").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("wc_out").build(), job);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void TestMap() throws IOException, ClassNotFoundException, InterruptedException {

    // prepare test data
    MapUTContext mapContext = new MapUTContext();
    mapContext.setInputSchema(INPUT_SCHEMA);
    mapContext.setOutputSchema(OUTPUT_SCHEMA, job);

    File inputDir = new File("src/test/resources/data/binary_wc_in");
    mapContext.addInputRecordsFromDir(inputDir);
    // run mapper
    TaskOutput output = runMapper(job, mapContext);

    // verify mapper outputs
    List<KeyValue<Record, Record>> kvs = output.getOutputKeyValues();
    Assert.assertEquals(4, kvs.size());
    Assert.assertEquals(new KeyValue<String, Long>(new String("c"), new Long(1)),
        new KeyValue<String, Long>((String) (kvs.get(0).getKey().get(0)), (Long) (kvs.get(0)
            .getValue().get(0))));
    Assert.assertEquals(new KeyValue<String, Long>(new String("hello"), new Long(1)),
        new KeyValue<String, Long>((String) (kvs.get(1).getKey().get(0)), (Long) (kvs.get(1)
            .getValue().get(0))));
    Assert.assertEquals(new KeyValue<String, Long>(new String("java"), new Long(1)),
        new KeyValue<String, Long>((String) (kvs.get(2).getKey().get(0)), (Long) (kvs.get(2)
            .getValue().get(0))));
    Assert.assertEquals(new KeyValue<String, Long>(new String("中国sss=$!="), new Long(1)),
        new KeyValue<String, Long>((String) (kvs.get(3).getKey().get(0)), (Long) (kvs.get(3)
            .getValue().get(0))));
  }

}
