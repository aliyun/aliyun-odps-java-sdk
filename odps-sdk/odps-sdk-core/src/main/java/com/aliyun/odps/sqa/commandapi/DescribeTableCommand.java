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

import org.apache.commons.lang.StringEscapeUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.Lists;

class DescribeTableCommand implements Command {

  private static final Map<String, TypeInfo> tableMap = new LinkedHashMap<>();

  static {
    tableMap.put("Owner", TypeInfoFactory.STRING);
    tableMap.put("Project", TypeInfoFactory.STRING);
    tableMap.put("Schema", TypeInfoFactory.STRING);
    tableMap.put("TableComment", TypeInfoFactory.STRING);
    tableMap.put("CreatedTime", TypeInfoFactory.DATETIME);
    tableMap.put("LastDDLTime", TypeInfoFactory.DATETIME);
    tableMap.put("LastModifiedTime", TypeInfoFactory.DATETIME);
    tableMap.put("Lifecycle", TypeInfoFactory.BIGINT);
    tableMap.put("TableType", TypeInfoFactory.STRING);
    tableMap.put("NativeColumns", TypeInfoFactory.getArrayTypeInfo(getStructTypeInfo()));
    tableMap.put("PartitionColumns", TypeInfoFactory.getArrayTypeInfo(getStructTypeInfo()));
    tableMap.put("MetadataJson", TypeInfoFactory.STRING);
  }

  private static final Map<String, TypeInfo> partitionMap = new LinkedHashMap<>();

  static {
    partitionMap.put("PartitionSize", TypeInfoFactory.BIGINT);
    partitionMap.put("CreatedTime", TypeInfoFactory.DATETIME);
    partitionMap.put("LastDDLTime", TypeInfoFactory.DATETIME);
    partitionMap.put("LastModifiedTime", TypeInfoFactory.DATETIME);
    partitionMap.put("MetadataJson", TypeInfoFactory.STRING);
  }

  private static final Map<String, TypeInfo> extendedTableMap = new LinkedHashMap<>(tableMap);

  static {
    extendedTableMap.replace("NativeColumns", TypeInfoFactory.getArrayTypeInfo(getExtendedStructTypeInfo()));
    extendedTableMap.put("ExtendedInfo", TypeInfoFactory
        .getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    extendedTableMap.put("ExtendedInfoJson", TypeInfoFactory.STRING);
  }

  private static final Map<String, TypeInfo>
      extendedPartitionMap =
      new LinkedHashMap<>(partitionMap);

  static {
    extendedPartitionMap.put("LifeCycle", TypeInfoFactory.BIGINT);
    extendedPartitionMap.put("IsExstore", TypeInfoFactory.BOOLEAN);
    extendedPartitionMap.put("IsArchived", TypeInfoFactory.BOOLEAN);
    extendedPartitionMap.put("PhysicalSize", TypeInfoFactory.BIGINT);
    extendedPartitionMap.put("FileNum", TypeInfoFactory.BIGINT);
    extendedPartitionMap.put("ExtendedInfoJson", TypeInfoFactory.STRING);
  }

  private String project;
  private String schema;
  private String table;
  private String partition;
  private boolean isExtended;

  public DescribeTableCommand(String project, String schema, String table, String partition,
                              boolean isExtended) {
    this.project = project;
    this.schema = schema;
    this.table = table;
    this.partition = partition;
    this.isExtended = isExtended;
  }

  @Override
  public boolean isSync() {
    return true;
  }

  private Map<String, TypeInfo> getResultTypeInfo() {
    if (partition == null) {
      return isExtended ? extendedTableMap : tableMap;
    }
    return isExtended ? extendedPartitionMap : partitionMap;
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return new ArrayList<>(getResultTypeInfo().values());
  }

  @Override
  public List<String> getResultHeaders() {
    return new ArrayList<>(getResultTypeInfo().keySet());
  }

  static StructTypeInfo getStructTypeInfo() {
    List<String> names = Lists.newArrayList("Field", "Type", "Comment");
    StructTypeInfo
        structTypeInfo =
        TypeInfoFactory.getStructTypeInfo(names, Lists
            .newArrayList(TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                          TypeInfoFactory.STRING));
    return structTypeInfo;
  }

  static StructTypeInfo getExtendedStructTypeInfo() {
    List<String>
        extendedNames =
        Lists.newArrayList("Field", "Type", "Nullable", "DefaultValue", "Comment");
    StructTypeInfo
        extendedStructTypeInfo =
        TypeInfoFactory.getStructTypeInfo(extendedNames, Lists
            .newArrayList(TypeInfoFactory.STRING, TypeInfoFactory.STRING,
                          TypeInfoFactory.BOOLEAN,
                          TypeInfoFactory.STRING, TypeInfoFactory.STRING));
    return extendedStructTypeInfo;
  }

  @Override
  public RecordIter<Record> run(Odps odps, CommandInfo commandInfo) throws OdpsException {

    boolean useOdpsNamespaceSchema = commandInfo.isOdpsNamespaceSchema();
    String schemaCopy = schema;
    schema = CommandUtil.getRealSchemaName(odps, project, schema, useOdpsNamespaceSchema);
    project = CommandUtil.getRealProjectName(odps, project, schemaCopy, useOdpsNamespaceSchema);

    Table t = odps.tables().get(project, schema, table);
    Map<String, Object> results;
    if (partition == null) {
      t.reload();
      results = isExtended ? getExtendedRows(t, null) : getRows(t, null);
    } else {
      if (partition.trim().length() == 0) {
        throw new OdpsException("Invalid partition key.");
      }

      Partition pt = t.getPartition(new PartitionSpec(partition));
      pt.reload();
      results = isExtended ? getExtendedRows(t, pt) : getRows(t, pt);
    }

    return new RecordIter<>(CommandUtil.toRecord(results, getResultTypeInfo()).iterator(),
                            getResultHeaders(), getResultTypes());
  }


  private Map<String, Object> getRows(Table t, Partition meta) {
    return getRows(t, meta, false);
  }

  private Map<String, Object> getRows(Table t, Partition meta, boolean isExtended) {
    Map<String, Object> map = new HashMap<>();

    try {
      if (meta != null) { // partition meta
        map.put("PartitionSize", meta.getSize());
        map.put("CreatedTime", meta.getCreatedTime());
        map.put("LastDDLTime", meta.getLastMetaModifiedTime());
        map.put("LastModifiedTime", meta.getLastDataModifiedTime());
        map.put("MetadataJson", meta.getMetadataJson());
      } else { // table meta
        map.put("Owner", t.getOwner());
        map.put("Project", t.getProject());
        map.put("Schema", t.getSchemaName());
        map.put("TableComment", t.getComment());
        map.put("CreatedTime", t.getCreatedTime());
        map.put("LastDDLTime", t.getLastMetaModifiedTime());
        map.put("LastModifiedTime", t.getLastDataModifiedTime());
        map.put("Lifecycle", t.getLife());

        if (t.isExternalTable()) {
          map.put("TableType", Table.TableType.EXTERNAL_TABLE.toString());
        } else if (t.isVirtualView()) {
          map.put("TableType", Table.TableType.VIRTUAL_VIEW.toString());
        } else if (t.isMaterializedView()) {
          map.put("TableType", Table.TableType.MATERIALIZED_VIEW.toString());
        } else {
          map.put("TableType", Table.TableType.MANAGED_TABLE.toString());
        }

        List<Struct> structList = new ArrayList<>();
        for (Column c : t.getSchema().getColumns()) {
          String fieldName = c.getName();
          String typeName = c.getTypeInfo().getTypeName().toLowerCase();
          String comment = c.getComment();

          if (isExtended) {
            String defaultValueStr = null;
            if (c.hasDefaultValue()) {
              defaultValueStr = c.getDefaultValue();
            }
            Struct
                struct =
                new SimpleStruct(getExtendedStructTypeInfo(), Lists
                    .newArrayList(fieldName, typeName, c.isNullable(), defaultValueStr, comment));
            structList.add(struct);
          } else {
            Struct
                struct =
                new SimpleStruct(getStructTypeInfo(),
                                 Lists.newArrayList(fieldName, typeName, comment));
            structList.add(struct);
          }
        }
        map.put("NativeColumns", structList);

        map.put("PartitionColumns", null);
        if (t.getSchema().getPartitionColumns().size() > 0) {
          List<Struct> partitionStruct = new ArrayList<>();
          for (Column c : t.getSchema().getPartitionColumns()) {
            Struct
                struct =
                new SimpleStruct(getStructTypeInfo(), Lists
                    .newArrayList(c.getName(), c.getTypeInfo().getTypeName().toLowerCase(),
                                  c.getComment()));
            partitionStruct.add(struct);
          }
          map.put("PartitionColumns", partitionStruct);
        }
        map.put("MetadataJson", t.getMetadataJson());
      }
    } catch (Exception e) {
      throw new RuntimeException("Invalid table schema.", e);
    }

    return map;
  }

  private Map<String, Object> getExtendedRows(Table t, Partition pt) {

    Map<String, Object> results = getRows(t, pt, true);

    if (t.isVirtualView()) {
      return results;
    }

    try {
      if (pt != null) {
        results.put("LifeCycle", pt.getLifeCycle());
        results.put("IsExstore", pt.isExstore());
        results.put("IsArchived", pt.isArchived());
        results.put("PhysicalSize", pt.getPhysicalSize());
        results.put("FileNum", pt.getFileNum());
        results.put("ExtendedInfoJson", pt.getExtendedInfoJson());
      } else {
        Map<String, String> extendedInfo = new LinkedHashMap<>();
        if (t.isExternalTable()) {
          extendedInfo.put("TableID", t.getTableID());
          extendedInfo.put("StorageHandler", t.getStorageHandler());
          extendedInfo.put("Location", t.getLocation());
          extendedInfo.put("Resources", t.getResources());
          if (t.getSerDeProperties() != null) {
            for (Map.Entry<String, String> entry : t.getSerDeProperties().entrySet()) {
              extendedInfo.put(entry.getKey(), StringEscapeUtils.escapeJava(entry.getValue()));
            }
          }
        } else if (!t.isVirtualView()) {
          if (t.isMaterializedView()) {
            extendedInfo.put("IsOutdated", String.valueOf(t.isMaterializedViewOutdated()));
          }
          extendedInfo.put("TableID", t.getTableID());
          extendedInfo.put("IsArchived", String.valueOf(t.isArchived()));
          extendedInfo.put("PhysicalSize", String.valueOf(t.getPhysicalSize()));
          extendedInfo.put("FileNum", String.valueOf(t.getFileNum()));
        }
        extendedInfo.put("CryptoAlgoName", t.getCryptoAlgoName());
        results.put("ExtendedInfo", extendedInfo);
        results.put("ExtendedInfoJson", t.getExtendedInfoJson());
      }
    } catch (Exception e) {
      throw new RuntimeException("Invalid table schema.", e);
    }

    return results;
  }

}
