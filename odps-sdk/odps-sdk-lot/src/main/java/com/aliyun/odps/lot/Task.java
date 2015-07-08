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

package com.aliyun.odps.lot;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.lot.common.ArgumentNullException;
import com.aliyun.odps.lot.common.TemporaryFunction;
import com.aliyun.odps.lot.operators.DataSource;
import com.aliyun.odps.lot.operators.Operator;
import com.aliyun.odps.task.LOTTask;

import apsara.odps.lot.Lot;
import apsara.odps.lot.Lottask;

//不支持不连通图
public class Task {

  private Odps odps;
  private String name;
  private Map<String, String> hints = new HashMap<String, String>();
  private Map<String, String> resourceAliasesForCurrentProject = new HashMap<String, String>();
  private List<DataSource> roots = new ArrayList<DataSource>();
  private Map<String, TemporaryFunction> tmpFunctions = new HashMap<String, TemporaryFunction>();

  public Task(Odps odps, String name) {
    if (odps == null) {
      throw new ArgumentNullException("odps");
    }

    if (name == null) {
      throw new ArgumentNullException("name");
    }

    this.odps = odps;
    this.name = name;
  }

  public void addTemporaryFunction(TemporaryFunction function) {
    if (tmpFunctions.containsKey(function.getName())) {
      throw new IllegalArgumentException(
          "You are adding temporary functions with the same name '" + function.getName() + "'.");
    }

    tmpFunctions.put(function.getName(), function);
  }

  //这个hint直接set到LOTTask.java的run参数中，安全起见，也生成在LOT pb中。
  //有两条路输入settings，相当邪恶。
  public void addHint(String key, String value) {
    if (key == null) {
      throw new ArgumentNullException("key");
    }

    if (value == null) {
      throw new ArgumentNullException("value");
    }

    hints.put(key, value);
  }

  public void addResourceAliasesForCurrentProject(String source, String alias) {
    if (source == null) {
      throw new ArgumentNullException("source");
    }

    if (alias == null) {
      throw new ArgumentNullException("alias");
    }

    resourceAliasesForCurrentProject.put(source, alias);
  }

  public void addRootOperator(DataSource root) {
    if (root == null) {
      throw new ArgumentNullException("root");
    }

    roots.add(root);
  }

  public Odps getOdps() {
    return odps;
  }

  public String getCurrentProject() {
    return odps.getDefaultProject();
  }

  public String getName() {
    return name;
  }

  public Map<String, String> getHints() {
    return hints;
  }

  public Map<String, String> getResourceAliasesForCurrentProject() {
    return resourceAliasesForCurrentProject;
  }

  public List<DataSource> getRoots() {
    return roots;
  }

  public Map<String, TemporaryFunction> getTmpFunctions() {
    return tmpFunctions;
  }

  public ByteArrayInputStream getTaskInputStream() throws OdpsException {
    validateGraph();

    Lottask.LotTask.Builder task = Lottask.LotTask.newBuilder();
    for (Map.Entry<String, String> item : hints.entrySet()) {
      task.addEnvironmentBuilder().setKey(item.getKey()).setValue(item.getValue());
    }

    Set<Operator> visited = new HashSet<Operator>();
    Lot.LogicalOperatorTree.Builder tree = Lot.LogicalOperatorTree.newBuilder();
    for (Operator root : roots) {
      genTree(tree, root, visited);
    }
    task.setLot(tree.build());

    for (Map.Entry<String, TemporaryFunction> func : tmpFunctions.entrySet()) {
      task.addTempFunctions(func.getValue().toProtoBuf());
    }

    Lottask.LotTask lotTask = task.build();
    System.out.println(lotTask.toString());
    return new ByteArrayInputStream(lotTask.toByteArray());
  }

  public Instance run() throws OdpsException {
    String tmpRes = "lot-" + UUID.randomUUID().toString();
    FileResource res = new FileResource();
    res.setName(tmpRes);
    res.setIsTempResource(true);
    ByteArrayInputStream stream = getTaskInputStream();
    odps.resources().create(odps.getDefaultProject(), res, stream);

    return LOTTask.run(odps, getCurrentProject(), tmpRes, false, getHints(),
                       getResourceAliasesForCurrentProject());
  }

  private void genTree(Lot.LogicalOperatorTree.Builder tree, Operator root, Set<Operator> visited) {
    if (!visited.contains(root)) {
      tree.addOperators(root.toProtoBuf());
      visited.add(root);
      for (Operator child : root.getChildren()) {
        genTree(tree, child, visited);
      }
    }
  }

  //对图做各种检查
  private void validateGraph() throws OdpsException {
    if (roots.size() == 0) {
      throw new OdpsException(
          "Invalid LOT task. You have to set one root operator at least for LOT.");
    }

    //omitted other validations.
  }
}
