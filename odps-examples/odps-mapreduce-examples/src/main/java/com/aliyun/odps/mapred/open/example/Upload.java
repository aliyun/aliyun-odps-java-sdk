/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.mapred.open.example;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * Upload
 * 
 * Import data from text file into table
 * 
 **/
public class Upload {

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

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {

    }

  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: Upload <import_txt> <out_table>");
      System.exit(2);
    }

    JobConf job = new JobConf();

    job.setMapperClass(UploadMapper.class);

    // 设置资源名字, 可以在map中通过jobconf获取到
    job.set("import.filename", args[0]);

    // maponly作业需要显式设置reducer的数目为0
    job.setNumReduceTasks(0);

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:bigint"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("value:string"));

    InputUtils.addTable(TableInfo.builder().tableName("mr_empty").build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);

    JobClient.runJob(job);
  }

}
