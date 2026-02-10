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
package com.aliyun.odps.table.read;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.table.record.accessor.ArrowToRecordConverter;
import com.aliyun.odps.type.TypeInfo;

/**
 * 将Arrow类型的ArrowReader（列存）转换成RecordReader（行存），用于读取数据。
 * 通过请求得到Arrow数据流的接口称为新接口，过去请求得到String类型数据, parse为Record的接口称为老接口
 * <p>
 * 由于时区问题，新接口使用Java8提供的无时区新类型
 * <table cellspacing=8>
 *   <tr>
 *     <td>ODPS Type</td>
 *     <td>New Type</td>
 *     <td>Old Type</td>
 *   </tr>
 *   <tr>
 *     <td>DATE</td>
 *     <td>LocalDate</td>
 *     <td>java.sql.Date</td>
 *   </tr>
 *   <tr>
 *     <td>DATETIME</td>
 *     <td>ZonedDateTime</td>
 *     <td>java.util.Date</td>
 *   </tr>
 *   <tr>
 *     <td>TIMESTAMP</td>
 *     <td>Instant</td>
 *     <td>java.sql.Timestamp</td>
 *   </tr>
 *   <tr>
 *     <td>DECIMAL</td>
 *     <td>BigDecimal</td>
 *     <td>BigDecimal(trim zero)</td>
 *   </tr>
 *   <tr>
 *     <td>STRING</td>
 *     <td>byte[]</td>
 *     <td>String</td>
 *   </tr>
 * </table>
 * <p>
 * 新类型除DATETIME类型以外，均不支持设置时区。
 * 本类提供了transformToLegacyType方法，将新接口的类型转换为旧接口的类型，并设置时区，以提供兼容性。
 * 通过useLegacyOutputFormat flag进行控制，并在read时进行转换。
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ArrowStreamRecordReader implements RecordReader {

  private final ArrowReader arrowReader;
  private final Deque<Record> records;
  private List<Column> columns;
  private boolean isExtensionArrowType;

  public ArrowStreamRecordReader(ArrowReader arrowReader, TableSchema tableSchema) {
    this(arrowReader, tableSchema, (Set<String>) null);
  }

  public ArrowStreamRecordReader(ArrowReader arrowReader, TableSchema tableSchema,
                                 List<String> columnFilter) {
    this(arrowReader, tableSchema, columnFilter == null ? null : new HashSet<>(columnFilter));
  }

  public ArrowStreamRecordReader(ArrowReader arrowReader, TableSchema tableSchema,
                                 Set<String> columnFilter) {
    this(arrowReader, tableSchema, columnFilter, false, false);
  }

  public ArrowStreamRecordReader(ArrowReader arrowReader, TableSchema tableSchema,
                                 Set<String> columnFilter, boolean withPartitionColumn, boolean isExtension) {
    this.arrowReader = arrowReader;
    this.isExtensionArrowType = isExtension;
    records = new ArrayDeque<>();

    columns = tableSchema.getColumns();
    if (withPartitionColumn) {
      columns.addAll(tableSchema.getPartitionColumns());
    }
    if (columnFilter != null) {
      columns =
          columns.stream().filter(column -> columnFilter.contains(column.getName()))
              .collect(Collectors.toList());
      if (columnFilter.isEmpty()) {
        throw new IllegalArgumentException("zero available columns selected, please check your column filter.");
      }
    }
  }

  public Record read() throws IOException {
    if (!records.isEmpty()) {
      return records.removeFirst();
    }
    if (arrowReader != null && arrowReader.nextBatch()) {
      VectorSchemaRoot vectorSchemaRoot = arrowReader.getCurrentValue();
      if (vectorSchemaRoot == null) {
        return null;
      }
      convertToRecord(vectorSchemaRoot);
      return records.removeFirst();
    }
    return null;
  }

  private void convertToRecord(VectorSchemaRoot vectorSchemaRoot)
    throws IOException {
    for (int rowId = 0; rowId < vectorSchemaRoot.getRowCount(); rowId++) {
      ArrayRecord arrayRecord = new ArrayRecord(columns.toArray(new Column[0]));
      for (int colIndex = 0; colIndex < columns.size(); colIndex++) {
        Column column = columns.get(colIndex);
        FieldVector vector = vectorSchemaRoot.getVector(column.getName());
        TypeInfo typeInfo = column.getTypeInfo();
        ArrowVectorAccessor
          columnVectorAccessor =
          ArrowToRecordConverter.createColumnVectorAccessor(vector, typeInfo, this.isExtensionArrowType, true);
        Object data = ArrowToRecordConverter.getData(columnVectorAccessor, typeInfo, rowId, this.isExtensionArrowType, true);
        arrayRecord.setWithoutValidation(colIndex, (Serializable) data);
      }
      records.addLast(arrayRecord);
    }
  }

  /**
   * @throws IllegalStateException if close arrow reader failed， which may cause memory leak
   */
  public void close() {
    if (arrowReader == null) {
      return;
    }
    try {
      arrowReader.close();
    } catch (IOException e) {
      throw new IllegalStateException("close arrow reader failed, which may cause memory leak");
    }
  }
}
