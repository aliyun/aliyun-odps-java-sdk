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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class WriteSchema extends TableSchema {

  private List<Column> systemColumns;

  /**
   * Maps dot-separated paths to column IDs for nested columns.
   *
   * <p>For top-level columns the key is the column name (e.g. {@code "c2"}).
   * For array elements the key is {@code "columnName.element"}.
   * For struct fields the key is {@code "columnName.fieldName"}.
   * For deeper nesting the pattern continues, e.g.
   * {@code "c2.element.f2"} for {@code array<struct<f2:blob>>}.
   *
   * <p>This map is populated by {@link WriteSchemaDeserializer} and allows
   * callers to obtain the column ID needed by
   * {@code TableArrowWriter.uploadBlob(columnId, data)} even when the BLOB
   * column is nested inside an ARRAY or STRUCT.
   */
  private Map<String, Long> nestedColumnIds = Collections.emptyMap();

  public List<Column> getSystemColumns() {
    return systemColumns;
  }

  public void setSystemColumns(List<Column> systemColumns) {
    this.systemColumns = systemColumns;
  }

  public Map<String, Long> getNestedColumnIds() {
    return nestedColumnIds;
  }

  public void setNestedColumnIds(Map<String, Long> nestedColumnIds) {
    this.nestedColumnIds = nestedColumnIds != null ? nestedColumnIds : Collections.emptyMap();
  }

  /**
   * Returns the column ID for the given dot-separated path, or {@code null}
   * if not found.
   *
   * @param path dot-separated path, e.g. {@code "c2"} or {@code "c2.element"} or
   *             {@code "c2.f2"}
   * @return the column ID, or {@code null}
   */
  public Long getNestedColumnId(String path) {
    return nestedColumnIds.get(path);
  }

  /**
   * Recursively finds all BLOB columns (top-level and nested) and returns a
   * map of dot-separated paths to column IDs.
   *
   * <p>For a table with columns {@code c1 BIGINT, c2 ARRAY<BLOB>}, the result
   * would contain {@code {"c2.element": <id>}}.
   * For {@code c2 STRUCT<f1:STRING, f2:BLOB>}, the result would contain
   * {@code {"c2.f2": <id>}}.
   *
   * @return an unmodifiable map of paths to column IDs for all BLOB columns
   */
  public Map<String, Long> findAllBlobColumnIds() {
    Map<String, Long> result = new LinkedHashMap<>();
    for (Column column : getColumns()) {
      collectBlobColumnIds(column.getName(), column.getTypeInfo(), column.getColumnId(),
                           result);
    }
    return Collections.unmodifiableMap(result);
  }

  private void collectBlobColumnIds(String path, TypeInfo typeInfo, Long columnId,
                                    Map<String, Long> result) {
    if (typeInfo == null) {
      return;
    }
    if (typeInfo.getOdpsType() == OdpsType.BLOB) {
      if (columnId != null) {
        result.put(path, columnId);
      }
      return;
    }
    if (typeInfo instanceof ArrayTypeInfo) {
      TypeInfo elemType = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
      String elemPath = path + ".element";
      Long elemId = nestedColumnIds.get(elemPath);
      collectBlobColumnIds(elemPath, elemType, elemId, result);
    } else if (typeInfo instanceof StructTypeInfo) {
      StructTypeInfo structType = (StructTypeInfo) typeInfo;
      List<String> fieldNames = structType.getFieldNames();
      List<TypeInfo> fieldTypes = structType.getFieldTypeInfos();
      for (int i = 0; i < fieldTypes.size(); i++) {
        String fieldPath = path + "." + fieldNames.get(i);
        Long fieldId = nestedColumnIds.get(fieldPath);
        collectBlobColumnIds(fieldPath, fieldTypes.get(i), fieldId, result);
      }
    }
  }

  @Override
  public List<Column> getAllColumns() {
    List<Column> allColumns = super.getAllColumns();
    allColumns.addAll(systemColumns);
    return allColumns;
  }
}
