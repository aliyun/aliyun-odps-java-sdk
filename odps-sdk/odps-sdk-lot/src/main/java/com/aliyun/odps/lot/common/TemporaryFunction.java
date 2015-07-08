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

public class TemporaryFunction {

  private String name;
  private List<Resource> resources;
  private String className;
  private Language language;

  public TemporaryFunction(String name, List<Resource> resources, String className,
                           Language language) {
    if (name == null) {
      throw new ArgumentNullException("name");
    }

    if (resources == null) {
      throw new ArgumentNullException("resources");
    }
    if (resources.size() == 0) {
      throw new IllegalArgumentException(
          "You must specify one resource at least for the temporary function '" + name + "' .");
    }

    if (className == null) {
      throw new ArgumentNullException("className");
    }

    this.name = name;
    this.resources = resources;
    this.className = className;
    this.language = language;
  }

  public String getName() {
    return name;
  }

  public List<Resource> getResources() {
    return resources;
  }

  public String getClassName() {
    return className;
  }

  public Language getLanguage() {
    return language;
  }

  public ExpressionProtos.TemporaryFunction toProtoBuf() {
    ExpressionProtos.TemporaryFunction.Builder
        builder =
        ExpressionProtos.TemporaryFunction.newBuilder();
    builder.setName(name);
    builder.setClassName(className);

    for (Resource res : resources) {
      ExpressionProtos.TemporaryFunction.Resources.Builder
          r =
          ExpressionProtos.TemporaryFunction.Resources.newBuilder();
      r.setProject(res.getProject());
      r.setResourceName(res.getName());
      builder.addResources(r.build());
    }

    builder.setLanguage(language.toProtoBuf());

    return builder.build();
  }
}
