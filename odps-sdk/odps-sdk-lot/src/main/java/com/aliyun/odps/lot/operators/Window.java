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
import com.aliyun.odps.lot.common.WindowFunction;

import apsara.odps.lot.Lot;
import apsara.odps.lot.WindowProtos;

public class Window extends Operator {

  public static class Function {

    private WindowFunction function;
    private String alias;

    public Function(WindowFunction function, String alias) {
      if (function == null) {
        throw new ArgumentNullException("function");
      }

      if (alias == null) {
        throw new ArgumentNullException("alias");
      }

      this.function = function;
      this.alias = alias;
    }

    public WindowFunction getFunction() {
      return function;
    }

    public String getAlias() {
      return alias;
    }

    public WindowProtos.Window.Functions toProtoBuf() {
      WindowProtos.Window.Functions.Builder builder = WindowProtos.Window.Functions.newBuilder();
      builder.setAlias(alias);
      builder.setFunction(function.toProtoBuf());
      return builder.build();
    }
  }

  private List<Function> functions;

  public List<Function> getFunctions() {
    return functions;
  }

  public Window(List<Function> functions) {
    if (functions == null) {
      throw new ArgumentNullException("functions");
    }

    if (functions.size() == 0) {
      throw new IllegalArgumentException(
          "You have to specify one window function at least for Window operator.");
    }

    this.functions = functions;
  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 1);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();
    WindowProtos.Window.Builder w = WindowProtos.Window.newBuilder();
    w.setId(getId());
    w.setParentId(getParents().get(0).getId());

    for (Function func : functions) {
      w.addFunctions(func.toProtoBuf());
    }

    builder.setWindow(w.build());

    return builder.build();
  }

}
