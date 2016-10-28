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
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import com.aliyun.odps.mapred.unittest.*;

public class ReadArchiveResourceTest extends MRUnitTest {
  private final static String INPUT_SCHEMA = "a:string,b:string";
  private final static String OUTPUT_SCHEMA = "k:string,v:bigint";
  private static JobConf job = new JobConf();

  public static class ArchiveMapper extends MapperBase {

    private void readCacheArchive(TaskContext context, String path) throws IOException {
      Record record = context.createOutputRecord();
      StringBuilder importdata = new StringBuilder();
      Iterable<BufferedInputStream> bufferedInputs = null;

      try {
        byte[] buffer = new byte[1024];
        int bytesRead = 0;

        String resourceName = context.getJobConf().get("test_archive");
        bufferedInputs =
            path.equals("all") ? context.readResourceArchiveAsStream(resourceName) : context
                .readResourceArchiveAsStream(resourceName, path);

        Iterator<BufferedInputStream> iter = bufferedInputs.iterator();
        while (iter != null && iter.hasNext()) {
          BufferedInputStream bufInput = iter.next();
          while ((bytesRead = bufInput.read(buffer)) != -1) {
            String chunk = new String(buffer, 0, bytesRead);
            importdata.append(chunk);
          }
        }

        String lines[] = importdata.toString().split("\n");
        for (int i = 0; i < lines.length; i++) {
          String[] ss = lines[i].split(",");
          record.set(0, new String(ss[0].trim()));
          record.set(1, new Long(Long.parseLong(ss[1].trim())));
          context.write(record);
        }
      } catch (IOException ex) {
        throw new IOException(ex);
      } finally {
      }
    }

    @Override
    public void setup(TaskContext context) throws IOException {
      // test for readCacheArchiveAsStream(String resourceName)
      readCacheArchive(context, "all");
      // test for readCacheArchiveAsStream(String resourceName, String relativePath)
      readCacheArchive(context, "archive_test/test/2.txt");
    }
 }

  public ReadArchiveResourceTest() {
    job.setMapperClass(ArchiveMapper.class);

    job.set("test_archive", "archive_test.tar.gz");

    job.setNumReduceTasks(0);

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("value:bigint"));

    InputUtils.addTable(TableInfo.builder().tableName("mr_empty").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName("archive_out").build(), job);
  }

  @Test
  public void TestMapReduce() throws IOException, ClassNotFoundException,
      InterruptedException {
    // prepare test data
    MapUTContext mapContext = new MapUTContext();
    mapContext.setInputSchema(INPUT_SCHEMA);
    mapContext.setOutputSchema(OUTPUT_SCHEMA, job);
    File archiveFile = new File("src/test/resources/data/archive_test.tar.gz");
    mapContext.setArchiveResource("archive_test.tar.gz", archiveFile);

    // run mapper
    TaskOutput mapOutput = runMapper(job, mapContext);

    // verify results
    Assert.assertEquals(6, mapOutput.getTotalRecordCount());
    Assert.assertTrue(equalRecords(new File("src/test/resources/data/archive_out"),
        mapOutput.getOutputRecords(), false));
  }
}
