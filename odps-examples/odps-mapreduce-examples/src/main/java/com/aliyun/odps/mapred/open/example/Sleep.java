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

import java.io.IOException;

import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;

public class Sleep {

  private static final String SLEEP_SECS = "sleep.secs";

  public static class MapperClass extends MapperBase {

    // 由于没有输入数据，map函数不会被执行，相关逻辑只能写在setup中
    @Override
    public void setup(TaskContext context) throws IOException {
      try {
        // 获取jobconf中设置的sleep秒数，进行sleep
        Thread.sleep(context.getJobConf().getInt(SLEEP_SECS, 1) * 1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: Sleep <sleep_secs>");
      System.exit(-1);
    }

    JobConf job = new JobConf();
    job.setMapperClass(MapperClass.class);
    // 这个实例也是一个MapOnly的，所以需要设置reducer个数为0
    job.setNumReduceTasks(0);
    // 由于没有输入表，mapper的个数需要由用户明确进行指定
    job.setNumMapTasks(1);
    job.set(SLEEP_SECS, args[0]);

    JobClient.runJob(job);
  }
}
