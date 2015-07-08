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

package com.aliyun.odps.lot.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.lot.common.AggregationFunction;
import com.aliyun.odps.lot.common.Reference;

import apsara.odps.lot.AggregateProtos;
import apsara.odps.lot.Lot;

public class Aggregate extends Operator {

  private List<Reference> groupByColumns;
  private Map<String, AggregationFunction> functions;

  public Aggregate(List<Reference> groupByColumns, Map<String, AggregationFunction> functions) {
    if (groupByColumns == null) {
      this.groupByColumns = new ArrayList<Reference>();
    } else {
      this.groupByColumns = groupByColumns;
    }

    if (functions == null) {
      this.functions = new HashMap<String, AggregationFunction>();
    } else {
      this.functions = functions;
    }
  }

  public List<Reference> getGroupByColumns() {
    return groupByColumns;
  }

  public Map<String, AggregationFunction> getFunctions() {
    return functions;
  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 1);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();

    AggregateProtos.Aggregate.Builder agg = AggregateProtos.Aggregate.newBuilder();
    agg.setId(getId());
    agg.setParentId(getParents().get(0).getId());

    for (Map.Entry<String, AggregationFunction> item : functions.entrySet()) {
      AggregateProtos.Aggregate.Functions.Builder
          func =
          AggregateProtos.Aggregate.Functions.newBuilder();
      func.setAlias(item.getKey());
      func.setFunction(item.getValue().toProtoBuf());
      agg.addFunctions(func);
    }

    for (Reference ref : groupByColumns) {
      agg.addGroupByColumns(ref.toProtoBuf().getReference());
    }

    builder.setAggregate(agg.build());
    return builder.build();
  }
}
