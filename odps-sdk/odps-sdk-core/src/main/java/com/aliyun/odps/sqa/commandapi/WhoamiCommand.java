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

package com.aliyun.odps.sqa.commandapi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

class WhoamiCommand implements Command {

  private static final Map<String, TypeInfo> infoMap = new LinkedHashMap<>();

  static {
    infoMap.put("DisplayName", TypeInfoFactory.STRING);
    infoMap.put("SourceIP", TypeInfoFactory.STRING);
    infoMap.put("VpcId", TypeInfoFactory.STRING);
    infoMap.put("Endpoint", TypeInfoFactory.STRING);
    infoMap.put("TunnelEndpoint", TypeInfoFactory.STRING);
    infoMap.put("Project", TypeInfoFactory.STRING);
    infoMap.put("Schema", TypeInfoFactory.STRING);
  }

  @Override
  public boolean isSync() {
    return true;
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return new ArrayList<>(infoMap.values());
  }

  @Override
  public List<String> getResultHeaders() {
    return new ArrayList<>(infoMap.keySet());
  }

  @Override
  public RecordIter<Record> run(Odps odps, CommandInfo commandInfo) throws OdpsException {

    SecurityManager sm = odps.projects().get().getSecurityManager();
    String result = sm.runQuery(commandInfo.getCommandText(), false);
    Map<String, Object> map = new HashMap<>();

    try {
      JsonObject js = new JsonParser().parse(result).getAsJsonObject();
      map.put("DisplayName", js.has("DisplayName") ? js.get("DisplayName").getAsString() : null);
      map.put("SourceIP", js.has("SourceIP") ? js.get("SourceIP").getAsString() : null);
      map.put("VpcId", js.has("VpcId") ? js.get("VpcId").getAsString() : null);
      map.put("Endpoint", odps.getEndpoint());
      // TODO: support specify tunnel quota from ..?
      map.put("TunnelEndpoint", odps.projects().get().getTunnelEndpoint(null));
      map.put("Project", odps.getDefaultProject());
      map.put("Schema", StringUtils.isNullOrEmpty(odps.getCurrentSchema()) ? "default"
                                                                           : odps.getCurrentSchema());
    } catch (JsonParseException e) {
      throw new RuntimeException("parse whoami error:" + e.getMessage());
    }

    return new RecordIter<>(CommandUtil.toRecord(map, infoMap).iterator(),
                            getResultHeaders(), getResultTypes());
  }
}
