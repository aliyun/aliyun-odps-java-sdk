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

import java.io.IOException;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.local.utils.TestUtils;

public class AllowNoInputPositive {

  public static class Mapper1 extends MapperBase {

    @Override
    public void setup(TaskContext context) throws IOException {
      System.err.println("Mapper1 running");
      context.getCounter("MyCounters", "EnterSetup").increment(1);
    }
  }

  public static void main(String[] args) throws OdpsException {
    JobConf job1 = new JobConf();
    job1.setMapperClass(Mapper1.class);

    job1.setNumReduceTasks(0);

    // job.setAllowNoInput(true); // new mr do not need this

    RunningJob rj = JobClient.runJob(job1);
    long counter = rj.getCounters().findCounter("MyCounters", "EnterSetup").getValue();
    int expect = job1.getInt("odps.stage.mapper.num", 1);
    if (counter != expect) {
      throw new RuntimeException("Incorrect counter, expect: " + expect + ", get: " + counter);
    }
  }

  @Test
  public void test() throws Exception {
    WareHouse wareHouse = WareHouse.getInstance();
    String project = TestUtils.odps_test_mrtask;
    TestUtils.setEnvironment(project);
    new AllowNoInputPositive().main(new String[0]);
  }
}
