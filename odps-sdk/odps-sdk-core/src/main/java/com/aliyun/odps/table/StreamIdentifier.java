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
import com.aliyun.odps.utils.OdpsCommonUtils;

/**
 * Identifier of stream object.
 */
public class StreamIdentifier implements Serializable {

  private final String project;
  private final String streamName;

  public static StreamIdentifier of(String project, String streamName) {
    return new StreamIdentifier(project, streamName);
  }

  public StreamIdentifier(String project, String streamName) {
    Preconditions.checkString(project, "Identifier project cannot be null");
    Preconditions.checkString(streamName, "Identifier streamName cannot be null");
    this.project = project;
    this.streamName = streamName;
  }

  public String getProject() {
    return project;
  }


  public String getStreamName() {
    return streamName;
  }

  @Override
  public String toString() {
    return OdpsCommonUtils.quoteRef(project) + "." + OdpsCommonUtils.quoteRef(streamName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamIdentifier that = (StreamIdentifier) o;
    return project.equals(that.project) && streamName.equals(that.streamName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(project, streamName);
  }
}
