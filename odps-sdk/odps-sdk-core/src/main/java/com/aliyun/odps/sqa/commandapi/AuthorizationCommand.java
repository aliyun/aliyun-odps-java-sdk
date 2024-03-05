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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class AuthorizationCommand implements Command {

  @Override
  public boolean isSync() {
    return true;
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return Collections.singletonList(TypeInfoFactory.STRING);
  }

  @Override
  public List<String> getResultHeaders() {
    return Collections.singletonList("Info");
  }

  @Override
  public RecordIter<Record> run(Odps odps, CommandInfo commandInfo) throws OdpsException {

    Map<String, String> settings = new HashMap<>();
    settings.put("odps.namespace.schema", String.valueOf(commandInfo.isOdpsNamespaceSchema()));

    if (commandInfo.getHint().containsKey("odps.sql.allow.namespace.schema")) {
      settings.put("odps.sql.allow.namespace.schema",
                   String.valueOf(commandInfo.isOdpsNamespaceSchema()));
    }

    settings.put("odps.default.schema", odps.getCurrentSchema());

    SecurityManager sm = odps.projects().get().getSecurityManager();
    SecurityManager.AuthorizationQueryInstance
        instance =
        sm.run(commandInfo.getCommandText(), false, null, settings);

    instance.waitForSuccess();
    String result = instance.getResult();

    return new RecordIter<>(CommandUtil.toRecord(result, getResultHeaders().get(0)).iterator(),
                            getResultHeaders(),
                            getResultTypes());
  }
}
