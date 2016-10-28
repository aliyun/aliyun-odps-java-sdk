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

package com.aliyun.odps.mapred.unittest;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.unittest.*;

public class UploadTest extends MRUnitTest {

  private final static String OUTPUT_SCHEMA = "a:bigint,b:string";

  private JobConf job;

  public static class UploadMapper extends MapperBase {
    @Override
    public void setup(TaskContext context) throws IOException {
      Record record = context.createOutputRecord();
      StringBuilder importdata = new StringBuilder();
      BufferedInputStream bufferedInput = null;

      try {
        byte[] buffer = new byte[1024];
        int bytesRead = 0;

        String filename = context.getJobConf().get("import.filename");
        bufferedInput = context.readResourceFileAsStream(filename);

        while ((bytesRead = bufferedInput.read(buffer)) != -1) {
          String chunk = new String(buffer, 0, bytesRead);
          importdata.append(chunk);
        }

        String lines[] = importdata.toString().split("\n");
        for (int i = 0; i < lines.length; i++) {
          String[] ss = lines[i].split(",");
          record.set(0, Long.parseLong(ss[0].trim()));
          record.set(1, ss[1].trim());
          context.write(record);
        }
      } catch (FileNotFoundException ex) {
        throw new IOException(ex);
      } catch (IOException ex) {
        throw new IOException(ex);
      } finally {
      }
    }
  }

  public UploadTest() throws IOException {
    job = new JobConf();
    job.setMapperClass(UploadMapper.class);
    job.set("import.filename", "a.txt");
    job.setNumMapTasks(1);
    job.setNumReduceTasks(0);
    OutputUtils.addTable(TableInfo.builder().tableName("upload_out").build(), job);
  }

  @Test
  public void TestMap() throws IOException, ClassNotFoundException, InterruptedException {

    // prepare test data
    MapUTContext context = new MapUTContext();
    context.setOutputSchema(OUTPUT_SCHEMA, job);

    StringBuffer buf = new StringBuffer();
    buf.append("1,chinese\n");
    buf.append("2,中文");
    context.setFileResource("a.txt", buf.toString());

    // run mapper
    TaskOutput output = runMapper(job, context);

    // verify output
    List<Record> records = output.getOutputRecords();
    Assert.assertEquals(2, records.size());
    Assert.assertEquals(new Long(1), records.get(0).get("a"));
    Assert.assertEquals(new String("chinese"), records.get(0).get("b"));
    Assert.assertEquals(new Long(2), records.get(1).get("a"));
    Assert.assertEquals(new String("中文"), records.get(1).get("b"));
  }
}
