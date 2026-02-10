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

package com.aliyun.odps.table;

import java.io.Serializable;
import java.util.Objects;

import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.CommonUtils;

/**
 * Identifier of stream object.
 */
public class InstanceIdentifier implements Serializable {

  private final String project;
  private final String instanceId;

  public static InstanceIdentifier of(String project, String instanceId) {
    return new InstanceIdentifier(project, instanceId);
  }

  public InstanceIdentifier(String project, String instanceId) {
    Preconditions.checkString(project, "Identifier project cannot be null");
    Preconditions.checkString(instanceId, "Identifier instanceId cannot be null");
    this.project = project;
    this.instanceId = instanceId;
  }

  public String getProject() {
    return project;
  }


  public String getInstanceId() {
    return instanceId;
  }

  @Override
  public String toString() {
    return CommonUtils.quoteRef(project) + "." + CommonUtils.quoteRef(instanceId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InstanceIdentifier that = (InstanceIdentifier) o;
    return project.equals(that.project) && instanceId.equals(that.instanceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(project, instanceId);
  }
}
