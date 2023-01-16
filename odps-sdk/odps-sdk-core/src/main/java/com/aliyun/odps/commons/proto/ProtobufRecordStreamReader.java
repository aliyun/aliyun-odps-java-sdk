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

package com.aliyun.odps.commons.proto;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import org.xerial.snappy.SnappyFramedInputStream;
import net.jpountz.lz4.LZ4FrameInputStream;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Survey;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.data.IntervalYearMonth;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.tunnel.TunnelTableSchema;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;

/**
 * @author chao.liu
 */
public class ProtobufRecordStreamReader implements RecordReader {

  private BufferedInputStream bin;
  private CodedInputStream in;
  private Column[] columns;
  private TableSchema schema;
  private long count;
  private long bytesReaded = 0;
  private Checksum crc = new Checksum();
  private Checksum crccrc = new Checksum();
  protected boolean shouldTransform = false;

  public ProtobufRecordStreamReader() {

  }

  public ProtobufRecordStreamReader(TableSchema schema, InputStream in)
      throws IOException {
    this(schema, null, in, new CompressOption());
  }

  public ProtobufRecordStreamReader(TableSchema schema, InputStream in, CompressOption option)
      throws IOException {
    this(schema, null, in, option);
  }

  public ProtobufRecordStreamReader(List<Column> columns, InputStream in,
                                    CompressOption option) throws IOException {
    bin = new BufferedInputStream(in);

    if (option != null) {
      if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
        this.in = CodedInputStream.newInstance(new InflaterInputStream(bin));
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_SNAPPY)) {
        this.in = CodedInputStream.newInstance(new SnappyFramedInputStream(bin));
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME)) {
        this.in = CodedInputStream.newInstance(new LZ4FrameInputStream(bin));
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_RAW)) {
        this.in = CodedInputStream.newInstance((bin));
      } else {
        throw new IOException("invalid compression option.");
      }
    } else {
      this.in = CodedInputStream.newInstance(bin);
    }
    this.in.setSizeLimit(Integer.MAX_VALUE);

    String schemaStr = readSchema();
    if (StringUtils.isNullOrEmpty(schemaStr)) {
      throw new IOException("Invalid response schema in header:" + schemaStr);
    }
    JsonObject tree = new JsonParser().parse(schemaStr).getAsJsonObject();
    this.schema = new TunnelTableSchema(tree);

    if (columns == null) {
      this.columns = schema.getColumns().toArray(new Column[0]);
    } else {
      Column[] tmpColumns = new Column[columns.size()];
      for (int i = 0; i < columns.size(); ++i) {
        tmpColumns[i] = schema.getColumn(columns.get(i).getName());
      }
      this.columns = tmpColumns;
    }
  }

  public ProtobufRecordStreamReader(TableSchema schema, List<Column> columns, InputStream in,
                                    CompressOption option) throws IOException {
    if (columns == null) {
      this.columns = schema.getColumns().toArray(new Column[0]);
    } else {
      Column[] tmpColumns = new Column[columns.size()];
      for (int i = 0; i < columns.size(); ++i) {
        tmpColumns[i] = schema.getColumn(columns.get(i).getName());
      }
      this.columns = tmpColumns;
    }

    bin = new BufferedInputStream(in);

    if (option != null) {
      if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
        this.in = CodedInputStream.newInstance(new InflaterInputStream(bin));
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_SNAPPY)) {
        this.in = CodedInputStream.newInstance(new SnappyFramedInputStream(bin));
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME)) {
        this.in = CodedInputStream.newInstance(new LZ4FrameInputStream(bin));
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_RAW)) {
        this.in = CodedInputStream.newInstance((bin));
      } else {
        throw new IOException("invalid compression option.");
      }
    } else {
      this.in = CodedInputStream.newInstance(bin);
    }
    this.in.setSizeLimit(Integer.MAX_VALUE);
  }

  public void setTransform(boolean shouldTransform) {
    this.shouldTransform = shouldTransform;
  }

  /**
   * 使用 reuse 的Record 读取数据
   * 当 reuseRecord 为 null 时，返回一个新的 Record 对象
   * 当 reuseRecord 非 null 时， 返回 reuseRecord 本身
   * 当数据读取完成， 返回 null
   *
   * @param reuseRecord
   * @return
   * @throws IOException
   */
  public Record read(Record reuseRecord) throws IOException {
    if (reuseRecord == null) {
      reuseRecord = new ArrayRecord(columns, false);
    } else {
      for (int i = 0; i < reuseRecord.getColumnCount(); ++i) {
        reuseRecord.set(i, null);
      }
    }

    while (true) {
      int checkSum = 0;

      if (in.isAtEnd()) {
        return null;
      }

      int i = getTagFieldNumber(in);
      if (i == ProtoWireConstant.TUNNEL_END_RECORD) {
        checkSum = (int) crc.getValue();
        if (in.readUInt32() != checkSum) {
          throw new IOException("Checksum invalid.");
        }
        crc.reset();
        crccrc.update(checkSum);
        break;
      }
      if (i == ProtoWireConstant.TUNNEL_META_COUNT) {
        if (count != in.readSInt64()) {
          throw new IOException("count does not match.");
        }

        if (ProtoWireConstant.TUNNEL_META_CHECKSUM != getTagFieldNumber(in)) {
          throw new IOException("Invalid stream.");
        }

        if ((int) crccrc.getValue() != in.readUInt32()) {
          throw new IOException("Checksum invalid.");
        }

        if (!in.isAtEnd()) {
          throw new IOException("Expect at the end of stream, but not.");
        }
        return null;
      }
      // tag index starts from 1.
      if (i > columns.length) {
        throw new IOException(
            "Invalid protobuf tag. Perhaps the datastream from server is crushed.");
      }

      crc.update(i);

      reuseRecord.set(i - 1, readField(columns[i - 1].getTypeInfo()));
    }
    bytesReaded += in.getTotalBytesRead();
    in.resetSizeCounter();
    count++;
    return reuseRecord;
  }

  /**
   * MCQA direct download专用接口
   * 从 stream 开头读取 schema 对象
   *
   * @return
   * @throws IOException
   */
  public String readSchema() throws IOException {
    String schemaJson = "";
    while (true) {
      int checkSum = 0;
      if (in.isAtEnd()) {
        throw new IOException("Read schema failed, empty stream.");
      }

      int i = getTagFieldNumber(in);
      if (i == ProtoWireConstant.SCHEMA_END_TAG) {
        checkSum = (int) crc.getValue();
        if (in.readUInt32() != checkSum) {
          throw new IOException("Checksum invalid.");
        }
        crc.reset();
        bytesReaded += in.getTotalBytesRead();
        in.resetSizeCounter();
        return schemaJson;
      }

      // tag:1  schema
      if (i > 1) {
        throw new IOException(
            "Invalid protobuf tag. Perhaps the datastream from server is crushed.");
      }

      crc.update(i);

      schemaJson = readString();
    }
  }

  private Object readField(TypeInfo type) throws IOException {
    switch (type.getOdpsType()) {
      case DOUBLE: {
        double v = in.readDouble();
        crc.update(v);
        return v;
      }
      case FLOAT: {
        float v = in.readFloat();
        crc.update(v);
        return v;
      }
      case BOOLEAN: {
        boolean v = in.readBool();
        crc.update(v);
        return v;
      }
      case BIGINT: {
        long v = in.readSInt64();
        crc.update(v);
        return v;
      }
      case INTERVAL_YEAR_MONTH: {
        long v = in.readSInt64();
        crc.update(v);
        return new IntervalYearMonth((int) v);
      }
      case INT: {
        long v = in.readSInt64();
        crc.update(v);
        return (int)v;
      }
      case SMALLINT: {
        long v = in.readSInt64();
        crc.update(v);
        return (short) v;
      }
      case TINYINT: {
        long v = in.readSInt64();
        crc.update(v);
        return (byte) v;
      }
      case STRING: {
        return readBytes();
      }
      case VARCHAR: {
        return new Varchar(readString());
      }
      case CHAR: {
        return new Char(readString());
      }
      case BINARY:{
        return new Binary(readBytes());
      }
      case DATETIME:{
        long v = in.readSInt64();
        crc.update(v);
        return shouldTransform ? DateUtils.ms2date(v, DateUtils.LOCAL_CAL).toInstant().atZone(ZoneId.systemDefault()) :
               Instant.ofEpochMilli(v).atZone(ZoneId.systemDefault());
      }
      case DATE: {
        long v = in.readSInt64();
        crc.update(v);
        // translate to sql.date
        return LocalDate.ofEpochDay(v);
      }
      case INTERVAL_DAY_TIME: {
        long time = in.readSInt64();
        int nano = in.readSInt32();
        crc.update(time);
        crc.update(nano);
        return new IntervalDayTime(time, nano);
      }
      case TIMESTAMP: {
        long time = in.readSInt64();
        int nano = in.readSInt32();
        crc.update(time);
        crc.update(nano);
        return Instant.ofEpochSecond(time, nano);
      }
      case DECIMAL: {
        int size = in.readRawVarint32();
        byte[] bytes = in.readRawBytes(size);
        crc.update(bytes, 0, bytes.length);
        BigDecimal decimal = new BigDecimal(new String(bytes, "UTF-8"));
        return decimal;
      }
      case ARRAY: {
        return readArray(((ArrayTypeInfo) type).getElementTypeInfo());
      }
      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) type;
        return readMap(mapTypeInfo.getKeyTypeInfo(), mapTypeInfo.getValueTypeInfo());
      }
      case STRUCT: {
        return readStruct(type);
      }
      default:
        throw new IOException("Unsupported type " + type.getTypeName());
    }
  }

  private String readString() throws IOException {
    byte[] bytes = readBytes();
    return new String(bytes, "utf-8");
  }

  private byte[] readBytes() throws IOException {
    int size = in.readRawVarint32();
    byte[] bytes = in.readRawBytes(size);
    crc.update(bytes, 0, bytes.length);
    bytesReaded += in.getTotalBytesRead();
    in.resetSizeCounter();

    return bytes;
  }

  static int getTagFieldNumber(CodedInputStream in) throws IOException {
    return WireFormat.getTagFieldNumber(in.readTag());
  }

  @Override
  public Record read() throws IOException {
    return read(null);
  }

  public Record createEmptyRecord() throws IOException {
    return new ArrayRecord(columns);
  }

  @Override
  public void close() throws IOException {
    if (bin != null) {
      bin.close();
    }
  }

  public TableSchema getTableSchema() {
    return schema;
  }

  public long getTotalBytes() {
    return bytesReaded;
  }

  public Struct readStruct(TypeInfo type) throws IOException {
    StructTypeInfo typeInfo = (StructTypeInfo) type;
    List<Object> values = new ArrayList<Object>();
    List<TypeInfo> fieldTypeInfos = typeInfo.getFieldTypeInfos();

    for (int i = 0; i < typeInfo.getFieldCount(); ++i) {
      if (in.readBool()) {
        values.add(null);
      } else {
        values.add(readField(fieldTypeInfos.get(i)));
      }
    }

    return new SimpleStruct(typeInfo, values);
  }

  public List readArray(TypeInfo type) throws IOException {
    OdpsType t = type.getOdpsType();

    int arraySize = in.readUInt32();
    List list = new ArrayList();

    for (int i = 0; i < arraySize; i++) {
      if (in.readBool()) {
        list.add(null);
      } else {
        list.add(readField(type));
      }
    }

    return list;
  }

  public Map readMap(TypeInfo keyType, TypeInfo valueType) throws IOException {
    List keyArray = readArray(keyType);
    List valueArray = readArray(valueType);
    if (keyArray.size() != valueArray.size()) {
      throw new IOException("Read Map error: key value does not match.");
    }

    Map map = new HashMap();
    for (int i = 0; i < keyArray.size(); i++) {
      map.put(keyArray.get(i), valueArray.get(i));
    }

    return map;
  }

  /**
   * remain this func to keep compatibility
   * The func param is OdpsType, so it cannot support complex types
   * @see #readArray(TypeInfo), it supports all types
   */
  @Survey
  public List readArray(OdpsType type) throws IOException {
    int arraySize = in.readUInt32();
    List list = null;

    switch (type) {
      case STRING: {
        list = new ArrayList<byte []>();

        for (int i = 0; i < arraySize; i++) {
          if (in.readBool()) {
            list.add(null);
          } else {
            int size = in.readRawVarint32();
            byte[] bytes = in.readRawBytes(size);
            crc.update(bytes, 0, bytes.length);
            list.add(bytes);
          }
        }
        break;
      }
      case BIGINT: {
        list = new ArrayList<Long>();

        for (int i = 0; i < arraySize; i++) {
          if (in.readBool()) {
            list.add(null);
          } else {
            Long value = in.readSInt64();
            crc.update(value);
            list.add(value);
          }
        }
        break;
      }
      case DOUBLE: {
        list = new ArrayList<Double>();

        for (int i = 0; i < arraySize; i++) {
          if (in.readBool()) {
            list.add(null);
          } else {
            Double value = in.readDouble();
            crc.update(value);
            list.add(value);
          }
        }
        break;

      }
      case BOOLEAN: {
        list = new ArrayList<Boolean>();
        for (int i = 0; i < arraySize; i++) {
          if (in.readBool()) {
            list.add(null);
          } else {
            Boolean value = in.readBool();
            crc.update(value);
            list.add(value);
          }
        }
        break;
      }
      default:
        throw new IOException("Unsupport array type. type :" + type);
    }

    return list;
  }

  /**
   * Remain this func to keep compatibility
   * The func param is OdpsType, so it cannot support complex types
   * @see #readMap(TypeInfo, TypeInfo), it supports all types
   */
  @Survey
  public Map readMap(OdpsType keyType, OdpsType valueType) throws IOException {
    List keyArray = readArray(keyType);
    List valueArray = readArray(valueType);
    if (keyArray.size() != valueArray.size()) {
      throw new IOException("Read Map error: key value does not match.");
    }

    Map map = new HashMap();
    for (int i = 0; i < keyArray.size(); i++) {
      map.put(keyArray.get(i), valueArray.get(i));
    }

    return map;
  }

}
