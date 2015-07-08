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

import java.util.ArrayList;
import java.util.List;

import apsara.odps.lot.ExpressionProtos;

public class ScalarFunction extends ScalarExpression {

  private String project;
  private String name;

  private List<ScalarExpression> parameters;

  public String getProject() {
    return project;
  }

  public String getName() {
    return name;
  }

  public List<ScalarExpression> getParameters() {
    return parameters;
  }

  @Override
  public ExpressionProtos.ScalarExpression toProtoBuf() {
    ExpressionProtos.ScalarExpression.Builder
        builder =
        ExpressionProtos.ScalarExpression.newBuilder();

    ExpressionProtos.ScalarFunction.Builder sf = ExpressionProtos.ScalarFunction.newBuilder();
    sf.setProject(project);
    sf.setName(name);
    for (ScalarExpression param : parameters) {
      if (param == null) {
        sf.addParameters(ScalarExpression.genNull());
      } else {
        sf.addParameters(param.toProtoBuf());
      }
    }

    builder.setExpression(sf.build());
    return builder.build();
  }

  public ScalarFunction(String project, String name, List<ScalarExpression> parameters) {
    if (project == null) {
      throw new ArgumentNullException("project");
    }

    if (name == null) {
      throw new ArgumentNullException("name");
    }

    if (parameters == null) {
      this.parameters = new ArrayList<ScalarExpression>();
    } else {
      this.parameters = parameters;
    }

    this.project = project;
    this.name = name;

  }
}
