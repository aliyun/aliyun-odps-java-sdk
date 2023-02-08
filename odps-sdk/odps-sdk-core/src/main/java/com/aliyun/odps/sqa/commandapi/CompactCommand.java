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

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.utils.StringUtils;

class CompactCommand extends MergeTaskCommand {

  private String compactType;

  public CompactCommand(String tableName, String partition, String compactType) {
    super(tableName, partition);
    this.compactType = compactType;
  }

  private boolean checkTransactional(String tablePart, Odps odps) {
    String projectName = odps.getDefaultProject();
    String tableName = tablePart.split("\\s+")[0];

    if (tableName.contains(".")) {
      tableName = tableName.split("\\.")[1];
    }

    return odps.tables().get(projectName, tableName).isTransactional();
  }

  @Override
  public boolean isSync() {
    return false;
  }

  @Override
  public RecordIter<Record> run(Odps odps, CommandInfo commandInfo) throws OdpsException {
    String tableInfo = getTableName() + " " + getPartition();
    if (!checkTransactional(tableInfo, odps)) {
      throw new OdpsException(tableInfo + " is not a transactional table.");
    }

    compactType = getCompactType(compactType.trim());
    if (compactType == null) {
      throw new RuntimeException("Compact type should be MAJOR or MINOR.");
    }

    String taskNamePrefix = "console_merge_task_";

    Map<String, String> settingMap = new HashMap<>();
    settingMap.put("odps.merge.txn.table.compact", compactType);
    settingMap.put("odps.merge.restructure.action", "hardlink");

    return mergeTaskRun(odps, commandInfo, taskNamePrefix, settingMap);
  }

  private static String getCompactType(String type) {
    if (StringUtils.isNullOrEmpty(type)) {
      return null;
    }

    if (type.equalsIgnoreCase("major")) {
      return "major_compact";
    } else if (type.equalsIgnoreCase("minor")) {
      return "minor_compact";
    }

    return null;
  }
}
