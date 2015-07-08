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

public class WindowFunction {

  public static class WindowingClause {

    private int preceding;
    private int following;

    public int getPreceding() {
      return preceding;
    }

    public int getFollowing() {
      return following;
    }

    public WindowingClause(int preceding, int following) {

      this.preceding = preceding;
      this.following = following;
    }

    public ExpressionProtos.WindowFunction.WindowingClause toProtoBuf() {
      ExpressionProtos.WindowFunction.WindowingClause.Builder builder =
          ExpressionProtos.WindowFunction.WindowingClause.newBuilder();
      builder.setBegin(preceding);
      builder.setEnd(following);
      return builder.build();
    }
  }

  public static class Order {

    private ScalarExpression expression;
    private boolean asc = true;

    public void setAsc(boolean asc) {
      this.asc = asc;
    }

    public ScalarExpression getExpression() {
      return expression;
    }

    public boolean isAsc() {
      return asc;
    }

    public Order(ScalarExpression expression, boolean asc) {
      if (expression == null) {
        throw new ArgumentNullException("expression");
      }

      this.expression = expression;
      this.asc = asc;
    }

    public Order(ScalarExpression expression) {
      if (expression == null) {
        throw new ArgumentNullException("expression");
      }

      this.expression = expression;
    }

    public ExpressionProtos.WindowFunction.OrderBy toProtoBuf() {
      ExpressionProtos.WindowFunction.OrderBy.Builder builder =
          ExpressionProtos.WindowFunction.OrderBy.newBuilder();
      builder.setExpression(expression.toProtoBuf());
      builder.setIsAsc(asc);

      return builder.build();
    }
  }

  private String project;
  private String name;
  private List<ScalarExpression> parameters;
  private List<ScalarExpression> partitionBy;
  private List<Order> sortBy;
  private WindowingClause windowing;
  private boolean distinct;

  public WindowFunction(String project, String name, List<ScalarExpression> parameters,
                        List<ScalarExpression> partitionBy, boolean isDistinct) {
    if (project == null) {
      throw new ArgumentNullException("project");
    }

    if (name == null) {
      throw new ArgumentNullException("name");
    }

    if (partitionBy == null) {
      throw new ArgumentNullException("partitionBy");
    }
    if (partitionBy.size() == 0) {
      throw new IllegalArgumentException(
          "You have to specify one item at least in 'partitionBy' parameter.");
    }

    if (parameters == null) {
      this.parameters = new ArrayList<ScalarExpression>();
    } else {
      this.parameters = parameters;
    }
    this.project = project;
    this.name = name;
    this.partitionBy = partitionBy;
    this.distinct = isDistinct;
  }

  public String getProject() {
    return project;
  }

  public String getName() {
    return name;
  }

  public List<ScalarExpression> getParameters() {
    return parameters;
  }

  public List<ScalarExpression> getPartitionBy() {
    return partitionBy;
  }

  public List<Order> getSortBy() {
    return sortBy;
  }

  public WindowingClause getWindowing() {
    return windowing;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void setSortBy(List<Order> sortBy) {
    if (sortBy == null) {
      throw new ArgumentNullException("sortBy");
    }

    this.sortBy = sortBy;
  }

  public void setWindowing(WindowingClause windowing) {
    if (windowing == null) {
      throw new ArgumentNullException("windowing");
    }

    this.windowing = windowing;
  }

  public ExpressionProtos.WindowFunction toProtoBuf() {
    ExpressionProtos.WindowFunction.Builder builder = ExpressionProtos.WindowFunction.newBuilder();
    builder.setProject(project);
    builder.setName(name);
    builder.setIsDistinct(distinct);

    for (ScalarExpression param : parameters) {
      builder.addParameters(param.toProtoBuf());
    }

    for (ScalarExpression pb : partitionBy) {
      builder.addPartitionBy(pb.toProtoBuf());
    }

    if (sortBy != null && sortBy.size() > 0) {
      for (Order order : sortBy) {
        builder.addOrderBy(order.toProtoBuf());
      }
    }

    if (windowing != null) {
      builder.setWindowingClause(windowing.toProtoBuf());
    }

    return builder.build();
  }
}
