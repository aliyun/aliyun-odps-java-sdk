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

package com.aliyun.odps.storage.internal.models;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.PartitionSpec;
import com.google.gson.annotations.SerializedName;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CreateTableWriteSessionRequest {

  @SerializedName("PartialPartitionSpec")
  private String partialPartitionSpec = "";

  @SerializedName("Flags")
  private Map<String, String> flags = new HashMap<>();

  public String getPartialPartitionSpec() {
    return partialPartitionSpec;
  }

  public void setPartialPartitionSpec(PartitionSpec partialPartitionSpec) {
    this.partialPartitionSpec = partialPartitionSpec.toString(false, true);
  }

  public Map<String, String> getFlags() {
    return flags;
  }

  public void setFlags(Map<String, String> flags) {
    this.flags = flags;
  }
}
