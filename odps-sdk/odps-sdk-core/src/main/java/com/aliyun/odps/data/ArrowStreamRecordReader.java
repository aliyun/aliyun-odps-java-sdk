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
package com.aliyun.odps.data;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.table.record.accessor.ArrowToRecordConverter;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
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
public class ArrowStreamRecordReader extends DefaultRecordReader {

  private final ArrowReader arrowReader;
  private final Deque<Record> records;
  private List<Column> columns;

  /**
   * 旧接口的默认时区为"Asia/Shanghai"时区
   */
  private ZoneId timeZone = ZoneId.of("Asia/Shanghai");
  private DateTimeFormatter timeStampFormat;
  private DateTimeFormatter dateTimeFormat;
  private DateFormat dateFormat;

  private boolean useLegacyOutputFormat = false;

  public ArrowStreamRecordReader(ArrowReader arrowReader, TableSchema tableSchema) {
    this(arrowReader, tableSchema, (Set<String>) null);
  }

  public ArrowStreamRecordReader(ArrowReader arrowReader, TableSchema tableSchema,
                                 List<String> columnFilter) {
    this(arrowReader, tableSchema, columnFilter == null ? null : new HashSet<>(columnFilter));
  }

  public ArrowStreamRecordReader(ArrowReader arrowReader, TableSchema tableSchema,
                                 Set<String> columnFilter) {
    // for compatibility
    super(new ByteArrayInputStream(new byte[0]), null);
    this.arrowReader = arrowReader;
    records = new ArrayDeque<>();

    columns = tableSchema.getColumns();
    columns.addAll(tableSchema.getPartitionColumns());
    if (columnFilter != null) {
      columns =
          columns.stream().filter(column -> columnFilter.contains(column.getName()))
              .collect(Collectors.toList());
      if (columnFilter.isEmpty()) {
        throw new IllegalArgumentException("zero available columns selected, please check your column filter.");
      }
    }
  }

  public void setTimeZone(ZoneId timeZone) {
    this.timeZone = timeZone;
  }

  public void setUseLegacyOutputFormat(boolean flag) {
    useLegacyOutputFormat = flag;
    timeStampFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");
    dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
  }

  @Override
  public Record read() throws IOException {
    if (!records.isEmpty()) {
      return records.removeFirst();
    }
    if (arrowReader != null && arrowReader.loadNextBatch()) {
      VectorSchemaRoot vectorSchemaRoot = arrowReader.getVectorSchemaRoot();
      if (vectorSchemaRoot.getRowCount() == 0) {
        return null;
      }
      convertToRecord(vectorSchemaRoot);
      return records.removeFirst();
    }
    return null;
  }

  @Override
  public List<String> readRaw() throws IOException {
    throw new UnsupportedOperationException("arrow reader do not support readRaw(), please use read() instead");
  }

  @Override
  public InputStream getRawStream() {
    throw new UnsupportedOperationException("arrow reader do not support getRawStream(), please use read() instead");
  }

  @Override
  public Column[] getSchema() {
    return columns.toArray(new Column[0]);
  }

  private void convertToRecord(VectorSchemaRoot vectorSchemaRoot)
      throws IOException {
    for (int rowId = 0; rowId < vectorSchemaRoot.getRowCount(); rowId++) {
      ArrayRecord arrayRecord = new ArrayRecord(columns.toArray(new Column[0]));
      for (Column column : columns) {
        FieldVector vector = vectorSchemaRoot.getVector(column.getName());
        TypeInfo typeInfo = column.getTypeInfo();
        ArrowVectorAccessor
            columnVectorAccessor =
            ArrowToRecordConverter.createColumnVectorAccessor(vector, typeInfo, true);
        Object data = ArrowToRecordConverter.getData(columnVectorAccessor, typeInfo, rowId, true);
        if (typeInfo.getOdpsType() == OdpsType.DATETIME) {
          data = data == null ? null : ((ZonedDateTime) data).withZoneSameInstant(timeZone);
        }
        if (useLegacyOutputFormat) {
          data = transformToLegacyType(data, typeInfo);
        }
        arrayRecord.set(column.getName(), data);
      }
      records.addLast(arrayRecord);
    }
  }

  /**
   * 将Record新类型数据转换为旧类型，并应用设置时区，以提供兼容旧版本的能力
   * 其中DATETIME和TIMESTAMP类型为兼容时区，使用toString -> parse 组合，来进行转换
   * 其原理是：当Instant转换为ZoneDateTime并转换为时区，Java会将时区变化补在offset上，并不会实际改变时间，
   * 因此当转换为Timestamp和Date时，会将offset去掉，导致时区设置失效。
   * 此举可以解决设置时区与系统时区不一致和1900年发生5分43秒offset跳变的问题（主要是后者，前者还可以通过计算时区修改时间的方式解决）
   * <p>
   * 由于时区坑，不推荐使用旧类型读取数据
   */
  private Object transformToLegacyType(Object data, TypeInfo typeInfo) {
    if (data == null) {
      return data;
    }
    switch (typeInfo.getOdpsType()) {
      case DATE:
        data = java.sql.Date.valueOf((LocalDate) data);
        break;
      case DATETIME:
        data =
            parseDatetime(dateTimeFormat.format(
                ((ZonedDateTime) data).withZoneSameInstant(timeZone).toLocalDateTime()));
        break;
      case TIMESTAMP:
        data =
            parseTimestamp(timeStampFormat.format(
                ((Instant) data).atZone(ZoneId.systemDefault()).withZoneSameInstant(timeZone)
                    .toLocalDateTime()));
        break;
      case STRING:
        if (data instanceof byte[]) {
          data = new String((byte[]) data);
        }
        break;
      case ARRAY:
        TypeInfo elementTypeInfo = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
        ((ArrayList<Object>) data).replaceAll(item -> transformToLegacyType(item, elementTypeInfo));
        break;
      case MAP:
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        HashMap<Object, Object> newMap = new HashMap<>();
        ((HashMap<Object, Object>) data).forEach((key, value) -> {
          key = transformToLegacyType(key, mapTypeInfo.getKeyTypeInfo());
          value = transformToLegacyType(value, mapTypeInfo.getValueTypeInfo());
          newMap.put(key, value);
        });
        data = newMap;
        break;
      case STRUCT:
        List<TypeInfo> fieldTypeInfos = ((StructTypeInfo) typeInfo).getFieldTypeInfos();
        List<Object> fieldValues = new ArrayList<>(fieldTypeInfos.size());
        SimpleStruct originStruct = (SimpleStruct) data;
        for (int index = 0; index < fieldTypeInfos.size(); index++) {
          fieldValues.add(
              transformToLegacyType(originStruct.getFieldValue(index), fieldTypeInfos.get(index)));
        }
        data = new SimpleStruct((StructTypeInfo) typeInfo, fieldValues);
        break;
      default:
    }
    return data;
  }

  private Date parseDatetime(String date) {
    try {
      return dateFormat.parse(date);
    } catch (ParseException e) {
      throw new IllegalArgumentException("parse date `" + date + "` error: " + e.getMessage(), e);
    }
  }

  /**
   * 将String parse为Timestamp类型
   */
  private Timestamp parseTimestamp(String st) {
    String[] splits = st.split("\\.");
    java.sql.Timestamp
        timestamp =
        new java.sql.Timestamp(parseDatetime(splits[0]).getTime());
    if (splits.length == 2 && !splits[1].isEmpty()) {
      String nanoValueStr = splits[1];
      // 9 is the max number of digits allowed for a nano value
      if (nanoValueStr.length() > 9) {
        nanoValueStr = nanoValueStr.substring(0, 9);
      } else if (nanoValueStr.length() < 9) {
        StringBuilder nanoValueStrBuilder = new StringBuilder();
        nanoValueStrBuilder.append(nanoValueStr);
        while (nanoValueStrBuilder.length() < 9) {
          nanoValueStrBuilder.append("0");
        }
        nanoValueStr = nanoValueStrBuilder.toString();
      }
      timestamp.setNanos(Integer.parseInt(nanoValueStr));
    }
    return timestamp;
  }

  /**
   * @throws IllegalStateException if close arrow reader failed， which may cause memory leak
   */
  @Override
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
