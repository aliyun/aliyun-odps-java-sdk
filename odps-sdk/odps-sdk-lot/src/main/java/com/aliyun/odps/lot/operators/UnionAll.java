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
import java.util.List;

import com.aliyun.odps.lot.common.IllegalOperationException;

import apsara.odps.lot.Lot;
import apsara.odps.lot.UnionAllProtos;

public class UnionAll extends Operator {

  private List<Operator> parents = new ArrayList<Operator>();

  public UnionAll() {
  }

  @Override
  protected void onBeAddedAsChild(Operator parent) throws IllegalOperationException {
    super.onBeAddedAsChild(parent);

    if (parents.indexOf(parent) != -1) {
      throw new IllegalOperationException("You add one parent twice for an Union operator.");
    }
    parents.add(parent);

  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();
    UnionAllProtos.UnionAll.Builder ua = UnionAllProtos.UnionAll.newBuilder();
    ua.setId(getId());
    for (Operator parent : getParents()) {
      ua.addParents(parent.getId());
    }

    builder.setUnionAll(ua.build());

    return builder.build();
  }
}
