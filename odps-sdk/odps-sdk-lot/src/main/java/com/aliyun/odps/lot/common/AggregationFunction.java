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

import java.util.List;

import apsara.odps.lot.ExpressionProtos;

public class AggregationFunction {

  private String project;
  private String name;
  private boolean distinct;
  private List<ScalarExpression> parameters;

  public String getProject() {
    return project;
  }

  public String getName() {
    return name;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public List<ScalarExpression> getParameters() {
    return parameters;
  }

  public AggregationFunction(String project, String name, boolean distinct,
                             List<ScalarExpression> parameters) {
    if (project == null) {
      throw new ArgumentNullException("project");
    }

    if (name == null) {
      throw new ArgumentNullException("name");
    }

    if (parameters == null) {
      throw new ArgumentNullException("parameters");
    }

    this.project = project;
    this.name = name;
    this.distinct = distinct;
    this.parameters = parameters;

  }

  public ExpressionProtos.AggregationFunction toProtoBuf() {
    ExpressionProtos.AggregationFunction.Builder
        builder =
        ExpressionProtos.AggregationFunction.newBuilder();
    builder.setProject(project);
    builder.setName(name);
    builder.setIsDistinct(distinct);
    for (ScalarExpression param : parameters) {
      if (param == null) {
        builder.addParameters(ScalarExpression.genNull());
      } else {
        builder.addParameters(param.toProtoBuf());
      }
    }

    return builder.build();
  }
}
