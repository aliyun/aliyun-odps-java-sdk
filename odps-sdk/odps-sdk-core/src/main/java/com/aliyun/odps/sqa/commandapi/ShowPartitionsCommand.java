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
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class ShowPartitionsCommand implements Command {

  private String project;
  private String schema;
  private String table;
  private String partition;

  public ShowPartitionsCommand(String project, String schema, String table,
                               String partition) {
    this.project = project;
    this.schema = schema;
    this.table = table;
    this.partition = partition;
  }

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
    return Collections.singletonList("PartitionSpec");
  }

  @Override
  public RecordIter run(Odps odps, CommandInfo commandInfo) throws OdpsException {

    boolean useOdpsNamespaceSchema = commandInfo.isOdpsNamespaceSchema();
    String schemaCopy = schema;
    schema = CommandUtil.getRealSchemaName(odps, project, schema, useOdpsNamespaceSchema);
    project = CommandUtil.getRealProjectName(odps, project, schemaCopy, useOdpsNamespaceSchema);

    Table t = odps.tables().get(project, schema, table);
    Iterator<Partition> parts;
    if (partition != null) {
      parts = t.getPartitionIterator(new PartitionSpec(partition));
    } else {
      parts = t.getPartitionIterator();
    }

    return new PartitionRecordIter(parts, getResultHeaders(), getResultTypes());
  }

  static class PartitionRecordIter extends RecordIter<Partition> {

    PartitionRecordIter(Iterator<Partition> partitionIterator,
                        List<String> headers, List<TypeInfo> typeInfos) {
      super(partitionIterator, headers, typeInfos);
    }

    @Override
    Record toRecord(Partition partition) {
      String p = partition.getPartitionSpec().toString();
      p = p.replaceAll("\'", "");
      p = p.replaceAll(",", "/");
      Record record = new ArrayRecord(columns);
      record.set(0, p);
      return record;
    }
  }

}
