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

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.RunningJob;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

/**
 * This is the main class for creating a map/reduce job using Aggregate
 * framework. The Aggregate is a specialization of map/reduce framework,
 * specilizing for performing various simple aggregations.
 *
 * Generally speaking, in order to implement an application using Map/Reduce
 * model, the developer is to implement Map and Reduce functions (and possibly
 * combine function). However, a lot of applications related to counting and
 * statistics computing have very similar characteristics. Aggregate abstracts
 * out the general patterns of these functions and implementing those patterns.
 * In particular, the package provides generic mapper/redducer/combiner classes,
 * and a set of built-in value aggregators, and a generic utility class that
 * helps user create map/reduce jobs using the generic class. The built-in
 * aggregators include:
 *
 * sum over numeric values count the number of distinct values compute the
 * histogram of values compute the minimum, maximum, media,average, standard
 * deviation of numeric values
 *
 * The developer using Aggregate will need only to provide a plugin class
 * conforming to the following interface:
 *
 * public interface ValueAggregatorDescriptor { public ArrayList<Entry>
 * generateKeyValPairs(Object key, Object value); public void
 * configure(JobConfjob); }
 *
 * The package also provides a base class, ValueAggregatorBaseDescriptor,
 * implementing the above interface. The user can extend the base class and
 * implement generateKeyValPairs accordingly.
 *
 * The primary work of generateKeyValPairs is to emit one or more key/value
 * pairs based on the input key/value pair. The key in an output key/value pair
 * encode two pieces of information: aggregation type and aggregation id. The
 * value will be aggregated onto the aggregation id according the aggregation
 * type.
 *
 * This class offers a function to generate a map/reduce job using Aggregate
 * framework. The function takes the following parameters: input directory spec
 * input format (text or sequence file) output directory a file specifying the
 * user plugin class
 */
public class ValueAggregatorJob {

  /**
   * Create an Aggregate based map/reduce job.
   *
   * @param args
   *     the arguments used for job creation. Generic hadoop arguments are
   *     accepted.
   * @return a JobConf object ready for submission.
   * @throws IOException
   * @see GenericOptionsParser
   */
  public JobConf createValueAggregatorJob(String args[]) throws IOException {

    if (args.length < 2) {
      System.out.println("usage: inputDirs outDir "
                         + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]");
      System.exit(1);
    }
    String inputTbl = args[0];
    String outputTbl = args[1];
    int numOfReducers = 1;
    if (args.length > 2) {
      numOfReducers = Integer.parseInt(args[2]);
    }

    JobConf job = new JobConf();

    job.setMapperClass(ValueAggregatorMapper.class);
    // job.setCombinerClass(ValueAggregatorCombiner.class);
    job.setReducerClass(ValueAggregatorReducer.class);

    job.setMapOutputKeySchema(SchemaUtils.fromString("key:string"));
    job.setMapOutputValueSchema(SchemaUtils.fromString("value:string"));

    InputUtils.addTable(TableInfo.builder().tableName(inputTbl).build(), job);
    OutputUtils.addTable(TableInfo.builder().tableName(outputTbl).build(), job);

    job.setNumReduceTasks(numOfReducers);

    return job;
  }

  public JobConf createValueAggregatorJob(String args[],
                                          Class<? extends ValueAggregatorDescriptor>[] descriptors)
      throws IOException {
    JobConf job = createValueAggregatorJob(args);
    setAggregatorDescriptors(job, descriptors);
    return job;
  }

  public void setAggregatorDescriptors(JobConf job,
                                       Class<? extends ValueAggregatorDescriptor>[] descriptors) {
    job.setInt("aggregator.descriptor.num", descriptors.length);
    // specify the aggregator descriptors
    for (int i = 0; i < descriptors.length; i++) {
      job.set("aggregator.descriptor." + i,
              "UserDefined," + descriptors[i].getName());
    }
  }

  /**
   * create and run an Aggregate based map/reduce job.
   *
   * @param args
   *     the arguments used for job creation
   * @throws IOException
   */
  public static void main(String args[]) throws Exception {
    JobConf job = new JobConf();
    RunningJob rj = JobClient.runJob(job);
    rj.waitForCompletion();
  }

}
