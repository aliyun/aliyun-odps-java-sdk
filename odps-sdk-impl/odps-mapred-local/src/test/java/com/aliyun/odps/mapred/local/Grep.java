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

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.lib.IdentityReducer;
import com.aliyun.odps.mapred.local.lib.InverseMapper;
import com.aliyun.odps.mapred.local.lib.LongSumReducer;
import com.aliyun.odps.mapred.local.lib.RegexMapper;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * Extracts matching regexs from input files and counts them.
 */
public class Grep {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[4];
    args[0] = TestUtils.getInputTableName(this);
    args[1] = "grep_temp";
    args[2] = TestUtils.getOutputTableName(this);
    args[3] = "\\w+";

    if (args.length < 4) {
      System.err.println("Grep <inDir> <tmpDir> <outDir> <regex> [<group>]");
      System.exit(2);
    }

    JobConf grepJob = new JobConf();

    grepJob.setMapperClass(RegexMapper.class);
    grepJob.setReducerClass(LongSumReducer.class);

    grepJob.setMapOutputKeySchema(SchemaUtils.fromString("word:string"));
    grepJob.setMapOutputValueSchema(SchemaUtils.fromString("count:bigint"));

    InputUtils
        .addTable(TableInfo.builder().tableName(args[0]).build(), grepJob);
    OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(),
                         grepJob);

    grepJob.set("mapred.mapper.regex", args[3]);
    if (args.length == 5) {
      grepJob.set("mapred.mapper.regex.group", args[4]);
    }

    RunningJob rjGrep = JobClient.runJob(grepJob);
    rjGrep.waitForCompletion();
    rjGrep.getJobStatus().getState();

    JobConf sortJob = new JobConf();

    sortJob.setMapperClass(InverseMapper.class);
    sortJob.setReducerClass(IdentityReducer.class);

    sortJob.setMapOutputKeySchema(SchemaUtils.fromString("count:bigint"));
    sortJob.setMapOutputValueSchema(SchemaUtils.fromString("word:string"));

    InputUtils
        .addTable(TableInfo.builder().tableName(args[1]).build(), sortJob);
    OutputUtils.addTable(TableInfo.builder().tableName(args[2]).build(),
                         sortJob);

    sortJob.setNumReduceTasks(1); // write a single file
    sortJob.setOutputKeySortColumns(new String[]{"count"}); // sort by
    // decreasing
    // freq

    RunningJob rjSort = JobClient.runJob(sortJob);
    rjSort.waitForCompletion();
  }

}
