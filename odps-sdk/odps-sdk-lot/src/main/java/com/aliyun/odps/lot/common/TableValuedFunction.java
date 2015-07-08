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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import apsara.odps.lot.ExpressionProtos;

public class TableValuedFunction {

  private String name;
  private String project;
  private List<ScalarExpression> parameters;
  private List<String> outputColumns;
  private Map<String, String> properties = new HashMap<String, String>();

  public String getName() {
    return name;
  }

  public String getProject() {
    return project;
  }

  public List<ScalarExpression> getParameters() {
    return parameters;
  }

  public List<String> getOutputColumns() {
    return outputColumns;
  }

  public void addProperties(String key, String value) {
    properties.put(key, value);
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public TableValuedFunction(String name, String project, List<ScalarExpression> parameters,
                             List<String> outputColumns) {
    if (name == null) {
      throw new ArgumentNullException("name");
    }

    if (project == null) {
      throw new ArgumentNullException("project");
    }

    if (parameters == null) {
      this.parameters = new ArrayList<ScalarExpression>();
    } else {
      this.parameters = parameters;
    }

    if (outputColumns == null) {
      throw new ArgumentNullException("outputColumns");
    }

    if (outputColumns.size() == 0) {
      throw new IllegalArgumentException("You have to specify one output column at least.");
    }

    this.name = name;
    this.project = project;
    this.outputColumns = outputColumns;
  }

  public ExpressionProtos.TableValuedFunction toProtoBuf() {
    ExpressionProtos.TableValuedFunction.Builder
        builder =
        ExpressionProtos.TableValuedFunction.newBuilder();
    builder.setProject(project);
    builder.setName(name);

    for (ScalarExpression param : parameters) {
      if (param == null) {
        builder.addParameters(ScalarExpression.genNull());
      } else {
        builder.addParameters(param.toProtoBuf());
      }
    }

    for (String col : outputColumns) {
      builder.addOutputColumns(col);
    }

    for (Map.Entry<String, String> prop : properties.entrySet()) {
      ExpressionProtos.TableValuedFunction.Properties.Builder
          p =
          ExpressionProtos.TableValuedFunction.Properties.newBuilder();
      p.setKey(prop.getKey());
      p.setValue(prop.getValue());
      builder.addProperties(p.build());
    }

    return builder.build();
  }
}
