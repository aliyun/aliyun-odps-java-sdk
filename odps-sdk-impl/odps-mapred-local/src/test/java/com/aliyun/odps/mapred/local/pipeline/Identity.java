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
 * Identity
 *
 * @author yubin.byb
 */
public class Identity extends PipelineExampleBase {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.pipe);
  }

  @Test
  public void test() throws Exception {
    String[] args = new String[3];
    args[0] = "l_ss";
    args[1] = "pipe_identity_out";
    args[2] = "5";
    if (args.length != 2 && args.length != 3) {
      System.err
          .println("Usage: Identity <[project.]in_table> <[project.]out_table> [reducer_num]");
      System.exit(2);
    }

    JobConf job = new JobConf();
    Builder builder = Pipeline.builder();
    int nReducer = 2;

    if (args.length == 3) {
      nReducer = Integer.parseInt(args[2]);
    }

    // mapper
    builder
        .addMapper(IdentityMapper.class)
        .setOutputKeySchema(
            new Column[]{new Column("key", OdpsType.STRING), new Column("value", OdpsType.STRING)})
        .setOutputValueSchema(new Column[]{new Column("value", OdpsType.STRING)});

    // reducer
    for (int count = 0; count < nReducer; count++) {
      if (count < nReducer - 1) {
        builder
            .addReducer(IdentityIReducer.class)
            .setOutputKeySchema(
                new Column[]{new Column("key", OdpsType.STRING),
                             new Column("value", OdpsType.STRING)})
            .setOutputValueSchema(new Column[]{new Column("value", OdpsType.STRING)});
      } else {
        builder.addReducer(IdentityReducer.class);
      }
    }

    Pipeline pipeline = builder.createPipeline();

    addInputTable(job, args[0]);
    addOutputTable(job, args[1]);

    JobClient.runJob(job, pipeline);
  }
}
