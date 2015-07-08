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

import java.util.List;

import com.aliyun.odps.lot.common.ArgumentNullException;
import com.aliyun.odps.lot.common.TableValuedFunction;

import apsara.odps.lot.ApplyProtos;
import apsara.odps.lot.Lot;

public class Apply extends Operator {

  private List<TableValuedFunction> functions;

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 1);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();
    ApplyProtos.Apply.Builder apply = ApplyProtos.Apply.newBuilder();
    apply.setId(getId());
    apply.setParentId(getParents().get(0).getId());
    for (TableValuedFunction func : functions) {
      apply.addUdtfs(func.toProtoBuf());
    }

    builder.setApply(apply.build());
    return builder.build();
  }

  public Apply(List<TableValuedFunction> functions) {
    if (functions == null) {
      throw new ArgumentNullException("functions");
    }

    if (functions.size() == 0) {
      throw new IllegalArgumentException("You have to specify one table-valued function at least.");
    }

    this.functions = functions;
  }
}
