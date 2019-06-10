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

package com.aliyun.odps.mapred;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mapred.bridge.MetaExplorer;
import com.aliyun.odps.mapred.bridge.MockMetaExplorer;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.example.WordCount;

public class BridgeJobRunnerTest {

  @Test
  public void testSubmit() throws OdpsException {
    BridgeJobRunner runner = new BridgeJobRunner() {

      @Override
      protected Instance submitInternal() throws OdpsException {
        //System.err.println(Arrays.toString(job.getResources()));
        Assert.assertEquals(6, job.getResources().length);
        Assert.assertEquals("foo", job.getResources()[0]);

        Assert.assertEquals(5, aliasToTempResource.size());
        return null;
      }

      @Override
      protected MetaExplorer getMetaExplorer() {
        return new MockMetaExplorer();
      }
    };
    JobConf job = new JobConf();
    job.setMapperClass(WordCount.TokenizerMapper.class);
    job.setNumReduceTasks(0);
    job.setResources("foo,file:bar");
    runner.setConf(job);
    runner.submit();
  }

  @Test
  public void testMergeStreamJobAliasResource() throws OdpsException {
    BridgeJobRunner runner = new BridgeJobRunner() {

      @Override
      protected Instance submitInternal() throws OdpsException {
        Assert.assertEquals(6+2, job.getResources().length);
        Assert.assertEquals("foo", job.getResources()[0]);

        Assert.assertEquals(5+2, aliasToTempResource.size());
        return null;
      }

      @Override
      protected MetaExplorer getMetaExplorer() {
        return new MockMetaExplorer();
      }
    };
    JobConf job = new JobConf();
    job.setMapperClass(WordCount.TokenizerMapper.class);
    job.setNumReduceTasks(0);
    job.setResources("foo,file:bar");
    job.set(
        "stream.temp.resource.alias",
        "{\"count.py\":\"count_c1bd152b-6eef-4533-b334-bd4cb70eafe3_0.py\",\"words.py\":\"words_c1bd152b-6eef-4533-b334-bd4cb70eafe3_0.py\"}");

    runner.setConf(job);
    runner.submit();
  }
}
