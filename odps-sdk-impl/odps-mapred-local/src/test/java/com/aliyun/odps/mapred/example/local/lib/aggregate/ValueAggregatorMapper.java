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

package com.aliyun.odps.mapred.example.local.lib.aggregate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;

/**
 * This class implements the generic mapper of Aggregate.
 */
public class ValueAggregatorMapper extends MapperBase {

  protected ArrayList<ValueAggregatorDescriptor> aggregatorDescriptorList = null;
  private Record columnK, columnV;

  @Override
  public void setup(TaskContext context) throws IOException {
    JobConf job = context.getJobConf();
    this.initializeMySpec(job);
    this.logSpec();

    columnK = context.createMapOutputKeyRecord();
    columnV = context.createMapOutputValueRecord();
  }

  private static ValueAggregatorDescriptor getValueAggregatorDescriptor(
      String spec, JobConf job) {
    if (spec == null) {
      return null;
    }
    String[] segments = spec.split(",", -1);
    String type = segments[0];
    if (type.compareToIgnoreCase("UserDefined") == 0) {
      String className = segments[1];
      return new UserDefinedValueAggregatorDescriptor(className, job);
    }
    return null;
  }

  private static ArrayList<ValueAggregatorDescriptor> getAggregatorDescriptors(
      JobConf job) {
    String advn = "aggregator.descriptor";
    int num = job.getInt(advn + ".num", 0);
    ArrayList<ValueAggregatorDescriptor> retv = new ArrayList<ValueAggregatorDescriptor>(
        num);
    for (int i = 0; i < num; i++) {
      String spec = job.get(advn + "." + i);
      ValueAggregatorDescriptor ad = getValueAggregatorDescriptor(spec, job);
      if (ad != null) {
        retv.add(ad);
      }
    }
    return retv;
  }

  private void initializeMySpec(JobConf job) {
    this.aggregatorDescriptorList = getAggregatorDescriptors(job);
    if (this.aggregatorDescriptorList.size() == 0) {
      this.aggregatorDescriptorList
          .add(new UserDefinedValueAggregatorDescriptor(
              ValueAggregatorBaseDescriptor.class.getCanonicalName(), job));
    }
  }

  protected void logSpec() {

  }

  @Override
  public void cleanup(TaskContext context) throws IOException {

  }

  /**
   * the map function. It iterates through the value aggregator descriptor list
   * to generate aggregation id/value pairs and emit them.
   */
  @Override
  public void map(long recordNum, Record record, TaskContext context)
      throws IOException {
    // LongWritable key = new LongWritable(recordNum);

    Iterator iter = this.aggregatorDescriptorList.iterator();
    while (iter.hasNext()) {
      ValueAggregatorDescriptor ad = (ValueAggregatorDescriptor) iter.next();
      Iterator<Entry<String, String>> ens = ad.generateKeyValPairs(recordNum,
                                                                   record).iterator();
      while (ens.hasNext()) {
        Entry<String, String> en = ens.next();

        columnK.set(new Object[]{en.getKey()});
        columnV.set(new Object[]{en.getValue()});
        context.write(columnK, columnV);
      }
    }
  }

}
