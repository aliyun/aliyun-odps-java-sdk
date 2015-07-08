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

import apsara.odps.lot.Lot;

public abstract class Operator {

  private static int identity = 0;

  private String id;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Operator() {
    id = Integer.toString(genId());
  }

  public abstract Lot.LogicalOperator toProtoBuf();

  private static synchronized int genId() {
    return identity++;
  }

  private List<Operator> children = new ArrayList<Operator>();
  private List<Operator> parents = new ArrayList<Operator>();

  public void addChild(Operator child) throws IllegalArgumentException, IllegalOperationException {
    if (child == null) {
      throw new ArgumentNullException("child");
    }
    if (children.indexOf(child) == -1) {
      children.add(child);
      child.parents.add(this);
      onAddChild(child);
      child.onBeAddedAsChild(this);
    }
  }

  protected void onAddChild(Operator child) throws IllegalOperationException {
  }

  protected void onBeAddedAsChild(Operator parent) throws IllegalOperationException {
  }

  public List<Operator> getChildren() {
    return children;
  }

  public List<Operator> getParents() {
    return parents;
  }
}
