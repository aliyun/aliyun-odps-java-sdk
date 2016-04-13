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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.InflaterInputStream;

import org.xerial.snappy.SnappyFramedInputStream;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;

/**
 * @author chao.liu
 */
public class ProtobufRecordStreamReader implements RecordReader {

  private BufferedInputStream bin;
  private CodedInputStream in;
  private Column[] columns;
  private long count;
  private long bytesReaded = 0;
  private Checksum crc = new Checksum();
  private Checksum crccrc = new Checksum();

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
      reuseRecord = new ArrayRecord(columns);
    } else {
      for (int i = 0; i < reuseRecord.getColumnCount(); ++i) {
        reuseRecord.set(i, null);
      }
    }

    while (true) {
      int checkSum = 0;

      if (in.isAtEnd()) {
        throw new IOException("No more record");
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

      switch (columns[i - 1].getType()) {
        case DOUBLE: {
          double v = in.readDouble();
          crc.update(v);
          reuseRecord.setDouble(i - 1, v);
          break;
        }
        case BOOLEAN: {
          boolean v = in.readBool();
          crc.update(v);
          reuseRecord.setBoolean(i - 1, v);
          break;
        }
        case BIGINT: {
          long v = in.readSInt64();
          crc.update(v);
          reuseRecord.setBigint(i - 1, v);
          break;
        }
        case STRING: {
          int size = in.readRawVarint32();
          byte[] bytes = in.readRawBytes(size);
          crc.update(bytes, 0, bytes.length);
          reuseRecord.setString(i - 1, bytes);
          bytesReaded += in.getTotalBytesRead();
          in.resetSizeCounter();
          break;
        }
        case DATETIME: {
          long v = in.readSInt64();
          crc.update(v);
          reuseRecord.setDatetime(i - 1, DateUtils.ms2date(v));
          break;
        }
        case DECIMAL: {
          int size = in.readRawVarint32();
          byte[] bytes = in.readRawBytes(size);
          crc.update(bytes, 0, bytes.length);
          BigDecimal decimal = new BigDecimal(new String(bytes, "UTF-8"));
          reuseRecord.setDecimal(i - 1, decimal);
          break;
        }
        case ARRAY: {
          List<OdpsType> genericTypeList = columns[i - 1].getGenericTypeList();
          if ((genericTypeList == null) || (genericTypeList.isEmpty())) {
            throw new IOException("Failed to get OdpsType inside Array of column index: " + (i - 1));
          }

          if (reuseRecord instanceof ArrayRecord) {
            ((ArrayRecord) reuseRecord).setArray(i - 1, readArray(genericTypeList.get(0)));
          } else {
            throw new IOException("Only ArrayRecord support Array type: " + reuseRecord.getClass().getName());
          }
          break;
        }
        case MAP: {
          List<OdpsType> genericTypeList = columns[i - 1].getGenericTypeList();
          if ((genericTypeList == null) || (genericTypeList.isEmpty()) || (genericTypeList.size() < 2)) {
            throw new IOException("Failed to get OdpsType inside Map of column index: " + (i - 1));
          }

          if (reuseRecord instanceof ArrayRecord) {
            ((ArrayRecord) reuseRecord)
                .setMap(i - 1, readMap(genericTypeList.get(0), genericTypeList.get(1)));
          } else {
            throw new IOException("Only ArrayRecord support Map type: " + reuseRecord.getClass().getName());
          }
          break;
        }
        default:
          throw new IOException("Unsupported type " + columns[i - 1].getType());
      }
    }
    bytesReaded += in.getTotalBytesRead();
    in.resetSizeCounter();
    count++;
    return reuseRecord;
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

  public long getTotalBytes() {
    return bytesReaded;
  }

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
