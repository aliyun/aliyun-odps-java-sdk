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
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

/**
 * This class implements the generic reducer of Aggregate.
 */
public class ValueAggregatorReducer extends ReducerBase {

  private Record result = null;

  @Override
  public void setup(TaskContext context) throws IOException {
    result = context.createOutputRecord();
  }

  /**
   * @param key
   *     the key is expected to be a Text object, whose prefix indicates
   *     the type of aggregation to aggregate the values. In effect, data
   *     driven computing is achieved. It is assumed that each aggregator's
   *     getReport method emits appropriate output for the aggregator. This
   *     may be further customiized.
   * @value the values to be aggregated
   */
  @Override
  public void reduce(Record key, Iterator<Record> values, TaskContext context)
      throws IOException {
    String keyStr = key.get(0).toString();
    int pos = keyStr.indexOf(ValueAggregatorDescriptor.TYPE_SEPARATOR);
    String type = keyStr.substring(0, pos);
    keyStr = keyStr.substring(pos
                              + ValueAggregatorDescriptor.TYPE_SEPARATOR.length());

    ValueAggregator aggregator = ValueAggregatorBaseDescriptor
        .generateValueAggregator(type);
    while (values.hasNext()) {
      Record val = values.next();
      aggregator.addNextValue(val.get(0).toString());
    }

    String val = aggregator.getReport();

    result.set(0, keyStr);
    result.set(1, val);
    context.write(result);
  }

}
