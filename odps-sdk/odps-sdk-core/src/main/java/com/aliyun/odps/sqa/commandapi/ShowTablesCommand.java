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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableFilter;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class ShowTablesCommand implements Command {

  private static final Map<String, TypeInfo> showTablesMap = new LinkedHashMap<>();

  static {
    showTablesMap.put("Owner", TypeInfoFactory.STRING);
    showTablesMap.put("TableName", TypeInfoFactory.STRING);
  }

  private String project;
  private String schema;
  private String prefix;

  public ShowTablesCommand(String project, String schema, String prefix) {
    this.project = project;
    this.schema = schema;
    this.prefix = prefix;
  }

  @Override
  public boolean isSync() {
    return true;
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return new ArrayList<>(showTablesMap.values());
  }

  @Override
  public List<String> getResultHeaders() {
    return new ArrayList<>(showTablesMap.keySet());
  }

  @Override
  public RecordIter run(Odps odps, CommandInfo commandInfo) throws OdpsException {
    TableFilter prefixFilter = new TableFilter();
    prefixFilter.setName(prefix);

    boolean useOdpsNamespaceSchema = commandInfo.isOdpsNamespaceSchema();
    String schemaCopy = schema;
    schema = CommandUtil.getRealSchemaName(odps, project, schema, useOdpsNamespaceSchema);
    project = CommandUtil.getRealProjectName(odps, project, schemaCopy, useOdpsNamespaceSchema);

    Iterator<Table> it = odps.tables().iterator(project, schema, prefixFilter, false);
    return new TableRecordIter(it, getResultHeaders(), getResultTypes());
  }

  static class TableRecordIter extends RecordIter<Table> {

    TableRecordIter(Iterator<Table> it, List<String> headers, List<TypeInfo> typeInfos) {
      super(it, headers, typeInfos);
    }

    @Override
    Record toRecord(Table table) {
      Record record = new ArrayRecord(columns);
      record.set(0, table.getOwner());
      record.set(1, table.getName());
      return record;
    }
  }
}
