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

import java.util.Map;

import apsara.odps.PartitionSpecProtos;

public class PartitionSpecification {

  private Map<String, Constant> definition;

  public PartitionSpecification(Map<String, Constant> definition) {
    if (definition == null) {
      throw new ArgumentNullException("definition");
    }
    this.definition = definition;
  }

  public PartitionSpecProtos.PartitionSpec toProtoBuf() {
    PartitionSpecProtos.PartitionSpec.Builder
        builder =
        PartitionSpecProtos.PartitionSpec.newBuilder();

    for (Map.Entry<String, Constant> kv : definition.entrySet()) {
      PartitionSpecProtos.PartitionSpec.Items.Builder
          ps =
          PartitionSpecProtos.PartitionSpec.Items.newBuilder();
      ps.setKey(kv.getKey());
      ps.setValue(kv.getValue().toProtoBuf().getConstant());
      builder.addItems(ps.build());
    }

    return builder.build();
  }
}
