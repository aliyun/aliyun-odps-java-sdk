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

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.local.utils.TestUtils;
import com.aliyun.odps.pipeline.Pipeline;
import com.aliyun.odps.pipeline.Pipeline.Builder;

/**
 * This is an example ODPS Map/Reduce application. It reads the input table, map
 * each column into words and counts them. The output is a locally sorted list
 * of words and the count of how often they occurred.
 * <p>
 * To run: mapreduce -libjars mapreduce-examples.jar
 * com.aliyun.odps.mapreduce.examples.WordCount <i>in-tbl</i> <i>out-tbl</i>
 */
public class WordCount extends PipelineExampleBase {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[2];
    args[0] = "pipe_wordcount_in";
    args[1] = "pipe_wordcount_out";
    if (args.length != 2 && args.length != 3) {
      System.err.println("Usage: wordcount <[project.]in_table> <[project.]out_table> ");
      System.exit(2);
    }

    boolean hasIdentity = false;
    if (args.length == 3) {
      if (args[2].equalsIgnoreCase("Identity")) {
        hasIdentity = true;
      }
    }

    JobConf job = new JobConf();
    job.setInt("odps.mapred.local.record.limit", 1000);
    Builder builder = Pipeline.builder();

    // mapper
    builder.addMapper(TokenizerMapper.class)
        .setOutputKeySchema(new Column[]{new Column("word", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)})
        .setOutputKeySortColumns(new String[]{"word"})
        .setPartitionColumns(new String[]{"word"})
        .setOutputGroupingColumns(new String[]{"word"});

    if (hasIdentity) {
      // reducer1
      builder
          .addReducer(SumIReducer.class)
          .setOutputKeySchema(
              new Column[]{new Column("word", OdpsType.STRING),
                           new Column("count", OdpsType.BIGINT)})
          .setOutputValueSchema(new Column[]{new Column("count", OdpsType.BIGINT)});

      // reducer2
      builder.addReducer(IdentityReducer.class);
    } else {
      // reducer1
      builder.addReducer(SumReducer.class);
    }

    Pipeline pipeline = builder.createPipeline();
    
    job.setNumReduceTasks(3);

    addInputTable(job, args[0]);
    addOutputTable(job, args[1]);

    JobClient.runJob(job, pipeline);
  }
}
