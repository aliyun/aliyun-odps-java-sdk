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

package com.aliyun.odps.tunnel.io;

import java.io.IOException;
import java.io.InputStream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.tunnel.TunnelException;
@Deprecated
public class ReplicatorStatus {

  private String lastReplicatedPackId;
  private long lastReplicatedPackTimeStamp;

  public ReplicatorStatus(InputStream is) throws TunnelException, IOException {
    JSONObject tree;
    try {
      String json = IOUtils.readStreamAsString(is);
      tree = JSON.parseObject(json);
    } catch (Exception e) {
      throw new TunnelException("get last replicated pack id fail");
    }
    
    String node = tree.getString("LastReplicatedPackId");
    if (node != null) {
      lastReplicatedPackId = node;
    } else {
      throw new TunnelException("get last replicated packid fail");
    }

    Long node2 = tree.getLong("LastReplicatedPackTimeStamp");
    if (node2 != null) {
      lastReplicatedPackTimeStamp = node2.longValue();
    } else {
      throw new TunnelException("get last replicated pack timestamp fail");
    }
  }


  /**
   * 获得shard最后一个复制到离线集群的pack的id
   *
   */
  public String GetLastReplicatedPackId() {
    return lastReplicatedPackId;
  }

  /**
   * 获得shard最后一个复制到离线集群的pack的时间戳
   *
   */
  public long GetLastReplicatedPackTimeStamp() {
    return lastReplicatedPackTimeStamp;
  }
}
