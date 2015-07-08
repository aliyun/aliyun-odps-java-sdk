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
import com.aliyun.odps.lot.common.ScalarExpression;

import apsara.odps.lot.Lot;
import apsara.odps.lot.SelectProtos;

public class Select extends Operator {

  public static class Expression {

    private String alias;
    private ScalarExpression expression;

    public String getAlias() {
      return alias;
    }

    public ScalarExpression getExpression() {
      return expression;
    }

    public Expression(String alias, ScalarExpression expression) {
      if (alias == null) {
        throw new ArgumentNullException("alias");
      }

      if (expression == null) {
        throw new ArgumentNullException("expression");
      }

      this.alias = alias;
      this.expression = expression;
    }
  }

  private List<Expression> expressions;

  public List<Expression> getExpressions() {
    return expressions;
  }

  /*
  �����Ǹ�list������map����Ϊ������ĳЩ����������������alias�����������˸�FakeSink/AdhocSink
   */
  public Select(List<Expression> expressions) {
    if (expressions == null) {
      throw new ArgumentNullException("expressions");
    }

    if (expressions.size() == 0) {
      throw new IllegalArgumentException(
          "You have to specify one item at least for Select operator.");
    }

    this.expressions = expressions;
  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 1);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();
    SelectProtos.Select.Builder s = SelectProtos.Select.newBuilder();
    s.setId(getId());
    s.setParentId(getParents().get(0).getId());

    for (Expression expr : expressions) {
      SelectProtos.Select.Expressions.Builder e = SelectProtos.Select.Expressions.newBuilder();
      e.setExpression(expr.getExpression().toProtoBuf());
      e.setAlias(expr.getAlias());
      s.addExpressions(e.build());
    }

    builder.setSelect(s.build());

    return builder.build();
  }
}
