/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mapred.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.example.local.lib.aggregate.ValueAggregatorBaseDescriptor;
import com.aliyun.odps.mapred.example.local.lib.aggregate.ValueAggregatorJob;
import com.aliyun.odps.mapred.local.utils.TestUtils;

/**
 * This is an example Aggregated Hadoop Map/Reduce application. It reads the
 * text input files, breaks each line into words and counts them. The output is
 * a locally sorted list of words and the count of how often they occurred.
 *
 * To run: bin/hadoop jar hadoop-*-examples.jar aggregatewordcount <i>in-dir</i>
 * <i>out-dir</i> <i>numOfReducers</i> textinputformat
 */
public class AggregateWordCount {

  @Before
  public void setUp() throws Exception {
    TestUtils.setEnvironment(TestUtils.odps_test_mrtask);
  }

  public static class WordCountPlugInClass extends
                                           ValueAggregatorBaseDescriptor {

    @Override
    public ArrayList<Entry<String, String>> generateKeyValPairs(Object key,
                                                                Object val) {
      String countType = LONG_VALUE_SUM;
      ArrayList<Entry<String, String>> retv = new ArrayList<Entry<String, String>>();
      Record r = (Record) val;
      for (int i = 0; i < r.getColumnCount(); ++i) {
        Entry<String, String> e = generateEntry(countType, r.get(i).toString(),
                                                ONE);
        if (e != null) {
          retv.add(e);
        }
      }
      return retv;
    }
  }

  /**
   * The main driver for word count map/reduce program. Invoke this method to
   * submit the map/reduce job.
   *
   * @throws IOException
   *     When there is communication problems with the job tracker.
   */
  @Test
  public void test() throws Exception {
    String[] args = new String[2];
    args[0] = "aggregatewordcount_in";
    args[1] = "aggregatewordcount_out";
    ValueAggregatorJob aggrJob = new ValueAggregatorJob();

    JobConf conf = aggrJob.createValueAggregatorJob(args,
                                                    new Class[]{WordCountPlugInClass.class});

    JobClient.runJob(conf);
  }

}
