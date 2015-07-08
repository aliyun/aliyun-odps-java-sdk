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
import com.aliyun.odps.lot.common.Resource;
import com.aliyun.odps.lot.common.ScalarExpression;
import com.aliyun.odps.lot.common.Schema;

public abstract class Transform extends Operator {

  private List<ScalarExpression> parameters;
  private List<Resource> resources;
  private Schema schema;

  public List<ScalarExpression> getParameters() {
    return parameters;
  }

  public List<Resource> getResources() {
    return resources;
  }

  public Schema getSchema() {
    return schema;
  }

  public Transform(List<ScalarExpression> parameters, List<Resource> resources, Schema schema) {
    if (parameters == null) {
      this.parameters = new ArrayList<ScalarExpression>();
    } else {
      this.parameters = parameters;
    }

    if (schema == null) {
      throw new ArgumentNullException("schema");
    }

    if (resources == null) {
      this.resources = new ArrayList<Resource>();
    } else {
      this.resources = resources;
    }

    this.schema = schema;
  }
}
