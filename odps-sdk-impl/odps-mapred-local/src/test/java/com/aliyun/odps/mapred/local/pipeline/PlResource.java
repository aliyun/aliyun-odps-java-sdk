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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
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
 * Resource
 *
 * @author yubin.byb
 */
public class PlResource extends PipelineExampleBase {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

  public static class ResourceMapper extends MapperBase {

    @Override
    public void setup(TaskContext context) throws IOException {
    }

    @Override
    public void map(long key, Record record, TaskContext context) throws IOException {

    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }
  }

  public static class ResourceReducer extends ReducerBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      Record record = context.createOutputRecord();
      StringBuilder data = new StringBuilder();
      BufferedInputStream bufferedInput = null;
      String resFile = null;
      long total = 0;

      try {
        byte[] buffer = new byte[1024];
        int bytesRead = 0;

        resFile = context.getJobConf().get("resource.file");
        bufferedInput = context.readResourceFileAsStream(resFile);

        while ((bytesRead = bufferedInput.read(buffer)) != -1) {
          String chunk = new String(buffer, 0, bytesRead);
          data.append(chunk);
        }

        String lines[] = data.toString().split("\n");
        for (int i = 0; i < lines.length; i++) {
          total++;
        }
      } catch (FileNotFoundException ex) {
        throw new IOException(ex);
      } catch (IOException ex) {
        throw new IOException(ex);
      } finally {
        if (bufferedInput != null) {
          bufferedInput.close();
        }
      }

      record.set(new Object[]{resFile, total});
      context.write(record);
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context)
        throws IOException {

    }

    @Override
    public void cleanup(TaskContext context) throws IOException {
      Record record = context.createOutputRecord();
      String resTable = context.getJobConf().get("resource.table");
      long total = 0;

      Iterator<Record> iter = context.readResourceTable(resTable);
      while (iter.hasNext()) {
        Record r = iter.next();
        total++;
      }

      record.set(new Object[]{resTable, total});
      context.write(record);
    }
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[4];
    args[0] = "pipe_plresource_in";
    args[1] = "pipe_plresource_out";
    args[2] = "plresource.txt";
    args[3] = "table_resource1";
    if (args.length != 4) {
      System.err
          .println(
              "Usage: Identity <[project.]in_table> <[project.]out_table> <resource_file> <resource_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();
    Builder builder = Pipeline.builder();

    // mapper
    builder.addMapper(ResourceMapper.class)
        .setOutputKeySchema(new Column[]{new Column("key", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.STRING)});

    // reducer
    builder.addReducer(ResourceReducer.class);

    Pipeline pipeline = builder.createPipeline();

    job.set("resource.file", args[2]);
    job.set("resource.table", args[3]);

    job.setResources("plresource.txt");

    addInputTable(job, args[0]);
    addOutputTable(job, args[1]);

    JobClient.runJob(job, pipeline);
  }
}
