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

package com.aliyun.odps.lot.common;

import com.aliyun.odps.lot.operators.Operator;

import apsara.odps.lot.ExpressionProtos;

public class Reference extends ScalarExpression {

  private String name;
  private Operator from;

  public String getName() {
    return name;
  }

  public Operator getFrom() {
    return from;
  }

  public Reference(String name, Operator from) {

    this.name = name;
    this.from = from;
  }

  @Override
  public ExpressionProtos.ScalarExpression toProtoBuf() {
    ExpressionProtos.ScalarExpression.Builder
        builder =
        ExpressionProtos.ScalarExpression.newBuilder();
    apsara.odps.ExpressionProtos.Reference.Builder
        r =
        apsara.odps.ExpressionProtos.Reference.newBuilder();
    r.setName(name);
    r.setFrom(getFrom().getId());
    builder.setReference(r.build());
    return builder.build();
  }
}
