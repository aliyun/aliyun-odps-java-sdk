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

import com.aliyun.odps.lot.common.ArgumentNullException;
import com.aliyun.odps.lot.common.IllegalOperationException;
import com.aliyun.odps.lot.common.ScalarExpression;

import apsara.odps.JoinTypeProtos;
import apsara.odps.lot.JoinProtos;
import apsara.odps.lot.Lot;

public class Join extends Operator {

  public static enum Type {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    CartesianProduct
  }

  public static enum ExecutionHint {
    Auto,
    MergeJoin,
    FullHashJoin,
    ShuffledHashJoin,
    DistributedHashJoin
  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 2);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();
    JoinProtos.Join.Builder j = JoinProtos.Join.newBuilder();
    j.setId(getId());
    j.setLeftParentId(leftParent.getId());
    j.setRightParentId(rightParent.getId());

    switch (type) {
      case Inner:
        j.setJoinType(JoinTypeProtos.JoinType.InnerJoin);
        break;
      case LeftOuter:
        j.setJoinType(JoinTypeProtos.JoinType.LeftJoin);
        break;
      case RightOuter:
        j.setJoinType(JoinTypeProtos.JoinType.RightJoin);
        break;
      case FullOuter:
        j.setJoinType(JoinTypeProtos.JoinType.FullOuterJoin);
        break;
      case CartesianProduct:
        j.setJoinType(JoinTypeProtos.JoinType.CartesianProduct);
        break;
      default:
        assert (false);
    }

    if (onCondition != null) {
      j.setOnCondition(onCondition.toProtoBuf());
    }

    switch (executionHint) {
      case Auto:
        j.setExeType(JoinProtos.Join.ExecutionType.Auto);
        break;
      case MergeJoin:
        j.setExeType(JoinProtos.Join.ExecutionType.MergeJoin);
        break;
      case FullHashJoin:
        j.setExeType(JoinProtos.Join.ExecutionType.FullHashJoin);
        break;
      case ShuffledHashJoin:
        j.setExeType(JoinProtos.Join.ExecutionType.ShuffledHashJoin);
        break;
            /* not supported temporarily.
            case DistributedHashJoin:
                break;
            */
      default:
        assert (false);
    }

    j.addAllSmallParents(smallParents);

    builder.setJoin(j.build());
    return builder.build();
  }

  private Operator leftParent;
  private Operator rightParent;
  private Type type;
  private ScalarExpression onCondition;
  private ExecutionHint executionHint = ExecutionHint.Auto;
  private List<String> smallParents = new ArrayList<String>();

  public List<String> getSmallParents() {
    return smallParents;
  }

  public void setSmallParents(List<String> smallParents) {
    this.smallParents = smallParents;
  }

  public void setExecutionHint(ExecutionHint executionHint) {
    this.executionHint = executionHint;
  }

  public Operator getLeftParent() {

    return leftParent;
  }

  public Operator getRightParent() {
    return rightParent;
  }

  public Type getType() {
    return type;
  }

  public ScalarExpression getOnCondition() {
    return onCondition;
  }

  public ExecutionHint getExecutionHint() {
    return executionHint;
  }

  public Join(Operator leftParent, Operator rightParent, Type type, ScalarExpression onCondition)
      throws IllegalOperationException {
    if (leftParent == null) {
      throw new ArgumentNullException("leftParent");
    }

    if (rightParent == null) {
      throw new ArgumentNullException("rightParent");
    }

    this.leftParent = leftParent;
    this.rightParent = rightParent;

    leftParent.addChild(this);
    rightParent.addChild(this);

    this.type = type;
    this.onCondition = onCondition;
  }

  @Override
  protected void onBeAddedAsChild(Operator parent) throws IllegalOperationException {
    super.onBeAddedAsChild(parent);
    if (parent != leftParent && parent != rightParent) {
      throw new IllegalOperationException(
          "Unknown parent of join operator, it should be one of the parents you set in constructor.");
    }
  }
}
