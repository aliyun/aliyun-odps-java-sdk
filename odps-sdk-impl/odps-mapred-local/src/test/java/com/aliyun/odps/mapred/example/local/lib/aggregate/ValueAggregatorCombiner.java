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

//import java.io.IOException;
//import java.util.Iterator;

//import com.aliyun.odps.io.Text;
//import com.aliyun.odps.mapreduce.Combiner;
//import com.aliyun.odps.mapreduce.CombineContext;

/**
 * This class implements the generic combiner of Aggregate.
 */
public class ValueAggregatorCombiner /* extends Combiner<Text, Text> */ {

  /**
   * Combines values for a given key.
   *
   * @param key
   *          the key is expected to be a Text object, whose prefix indicates
   *          the type of aggregation to aggregate the values.
   * @param values
   *          the values to combine
   * @param output
   *          to collect combined values
   */
  // @Override
  // public void combine(Text key, Iterable<Text> values,
  // CombineContext<Text, Text> context) throws IOException,
  // InterruptedException {
  // String keyStr = key.toString();
  // int pos = keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR);
  // String type = keyStr.substring(0, pos);
  // ValueAggregator aggregator = ValueAggregatorBaseDescriptor
  // .generateValueAggregator(type);
  // for (Text t : values) {
  // aggregator.addNextValue(t);
  // }
  // Iterator outputs = aggregator.getCombinerOutput().iterator();
  //
  // while (outputs.hasNext()) {
  // Object v = outputs.next();
  // if (v instanceof Text) {
  // context.write(key, (Text) v);
  // } else {
  // context.write(key, new Text(v.toString()));
  // }
  // }
  // }

}
