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

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.data.AbstractChar;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.IntervalDayTime;
import com.aliyun.odps.data.IntervalYearMonth;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordPack;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import org.apache.commons.io.output.CountingOutputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * @author chao.liu
 */
public class ProtobufRecordStreamWriter implements RecordWriter {

  private CountingOutputStream bou;
  private Column[] columns;
  private CodedOutputStream out;
  private long count;

  private Checksum crc = new Checksum();
  private Checksum crccrc = new Checksum();
  private Deflater def;

  private boolean shouldTransform = false;

  public ProtobufRecordStreamWriter(TableSchema schema, OutputStream out) throws IOException {
    this(schema, out, new CompressOption());
  }

  public ProtobufRecordStreamWriter(TableSchema schema, OutputStream out, CompressOption option)
      throws IOException {
    columns = schema.getColumns().toArray(new Column[0]);
    OutputStream tmpOut;
    if (option != null) {
      if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
        def = new Deflater();
        def.setLevel(option.level);
        def.setStrategy(option.strategy);
        tmpOut = new DeflaterOutputStream(out, def);
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_SNAPPY)) {
        tmpOut = new SnappyFramedOutputStream(out);
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_RAW)) {
        tmpOut = out;
      } else {
        throw new IOException("invalid compression option.");
      }
    } else {
      tmpOut = out;
    }
    bou = new CountingOutputStream(tmpOut);
    this.out = CodedOutputStream.newInstance(bou);
  }

  static void writeRawBytes(byte[] value, CodedOutputStream out)
      throws IOException {
    out.writeRawVarint32(value.length);
    out.writeRawBytes(value);
  }


  public void setTransform(boolean shouldTransform) {
    this.shouldTransform = shouldTransform;
  }

  @Override
  public void write(Record r) throws IOException {

    int recordValues = r.getColumnCount();
    int columnCount = columns.length;
    if (recordValues > columnCount) {
      throw new IOException("record values more than schema.");
    }

    int i = 0;
    for (; i < columnCount && i < recordValues; i++) {

      Object v = r.get(i);
      if (v == null) {
        continue;
      }

      int pbIdx = i + 1;

      crc.update(pbIdx);

      TypeInfo typeInfo = columns[i].getTypeInfo();
      writeFieldTag(pbIdx, typeInfo);
      writeField(v, typeInfo);
    }

    int checksum = (int) crc.getValue();
    out.writeUInt32(ProtoWireConstant.TUNNEL_END_RECORD, checksum);

    crc.reset();
    crccrc.update(checksum);

    count++;
  }

  private void writeFieldTag(int pbIdx, TypeInfo typeInfo) throws IOException {
    switch (typeInfo.getOdpsType()) {
      case DATETIME:
      case BOOLEAN:
      case BIGINT:
      case TINYINT:
      case SMALLINT:
      case INT:
      case DATE:
      case INTERVAL_YEAR_MONTH: {
        out.writeTag(pbIdx, WireFormat.WIRETYPE_VARINT);
        break;
      }
      case DOUBLE: {
        out.writeTag(pbIdx, WireFormat.WIRETYPE_FIXED64);
        break;
      }
      case FLOAT: {
        out.writeTag(pbIdx, WireFormat.WIRETYPE_FIXED32);
        break;
      }
      case INTERVAL_DAY_TIME:
      case TIMESTAMP:
      case STRING:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case DECIMAL:
      case ARRAY:
      case MAP:
      case STRUCT:{
        out.writeTag(pbIdx, com.google.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED);
        break;
      }
      default:
        throw new IOException("Invalid data type: " + typeInfo);
    }
  }

  private void writeField(Object v, TypeInfo typeInfo) throws IOException {
    switch (typeInfo.getOdpsType()) {
      case BOOLEAN: {
        boolean value = (Boolean) v;
        crc.update(value);
        out.writeBoolNoTag(value);
        break;
      }
      case DATETIME: {
        Date value = (Date) v;


        Long longValue = null;
        if (!shouldTransform) {
          longValue = DateUtils.date2ms(value);
        } else {
          longValue = DateUtils.date2ms(value, DateUtils.LOCAL_CAL);
        }
        crc.update(longValue);
        out.writeSInt64NoTag(longValue);
        break;
      }
      case DATE: {
        Long longValue = DateUtils.getDayOffset((java.sql.Date) v);
        crc.update(longValue);
        out.writeSInt64NoTag(longValue);
        break;
      }
      case TIMESTAMP: {
        Integer nano = ((Timestamp) v).getNanos();
        Long value = (((Timestamp) v).getTime() - (nano / 1000000)) / 1000;
        crc.update(value);
        crc.update(nano);
        out.writeSInt64NoTag(value);
        out.writeSInt32NoTag(nano);
        break;
      }
      case INTERVAL_DAY_TIME: {
        Long value = ((IntervalDayTime) v).getTotalSeconds();
        Integer nano = ((IntervalDayTime) v).getNanos();
        crc.update(value);
        crc.update(nano);
        out.writeSInt64NoTag(value);
        out.writeSInt32NoTag(nano);
        break;
      }
      case VARCHAR:
      case CHAR: {
        byte [] bytes;
        bytes = ((AbstractChar) v).getValue().getBytes("UTF-8");
        crc.update(bytes, 0, bytes.length);
        writeRawBytes(bytes, out);
        break;
      }
      case STRING: {
        byte[] bytes;
        if (v instanceof String) {
          String value = (String) v;
          bytes = value.getBytes("UTF-8");
        } else {
          bytes = (byte[]) v;
        }
        crc.update(bytes, 0, bytes.length);
        writeRawBytes(bytes, out);
        break;
      }
      case BINARY: {
        byte[] bytes = ((Binary) v).data();

        crc.update(bytes, 0, bytes.length);
        writeRawBytes(bytes, out);
        break;
      }
      case DOUBLE: {
        double value = (Double) v;
        crc.update(value);
        out.writeDoubleNoTag(value);
        break;
      }
      case FLOAT: {
        float value = (Float) v;
        crc.update(value);
        out.writeFloatNoTag(value);
        break;
      }
      case BIGINT: {
        long value = (Long) v;
        crc.update(value);
        out.writeSInt64NoTag(value);
        break;
      }
      case INTERVAL_YEAR_MONTH: {
        long value = ((IntervalYearMonth) v).getTotalMonths();
        crc.update(value);
        out.writeSInt64NoTag(value);
        break;
      }
      case INT: {
        long value = ((Integer) v).longValue();
        crc.update(value);
        out.writeSInt64NoTag(value);
        break;
      }
      case SMALLINT: {
        long value = ((Short) v).longValue();
        crc.update(value);
        out.writeSInt64NoTag(value);
        break;
      }
      case TINYINT: {
        long value = ((Byte) v).longValue();
        crc.update(value);
        out.writeSInt64NoTag(value);
        break;
      }
      case DECIMAL: {
        String value = ((BigDecimal) v).toPlainString();
        byte[] bytes = value.getBytes("UTF-8");
        crc.update(bytes, 0, bytes.length);
        writeRawBytes(bytes, out);
        break;
      }
      case ARRAY: {
        writeArray((List) v, ((ArrayTypeInfo) typeInfo).getElementTypeInfo());
        break;
      }
      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;

        writeMap((Map) v, mapTypeInfo.getKeyTypeInfo(),
                 mapTypeInfo.getValueTypeInfo());
        break;
      }
      case STRUCT: {
        writeStruct((Struct) v, (StructTypeInfo) typeInfo);
        break;
      }
      default:
        throw new IOException("Invalid data type: " + typeInfo);
    }
  }

  private void writeStruct(Struct object, StructTypeInfo typeInfo) throws IOException {
    List<TypeInfo> fieldTypeInfos = typeInfo.getFieldTypeInfos();

    for (int i = 0; i < fieldTypeInfos.size(); ++i) {
      if (object.getFieldValue(i) == null) {
        out.writeBoolNoTag(true);
      } else {
        out.writeBoolNoTag(false);
        writeField(object.getFieldValue(i), fieldTypeInfos.get(i));
      }
    }
  }

  private void writeArray(List v, TypeInfo type) throws IOException {
    out.writeInt32NoTag(v.size());
    for (int i = 0; i < v.size(); i++) {
      if (v.get(i) == null) {
        out.writeBoolNoTag(true);
      } else {
        out.writeBoolNoTag(false);
        writeField(v.get(i), type);
      }
    }
  }

  private void writeMap(Map v, TypeInfo keyType, TypeInfo valueType) throws IOException {
    // note: storage will check the availability of key and value
    List keyList = new ArrayList();
    List valueList = new ArrayList();
    Iterator iter = v.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();

      keyList.add(entry.getKey());
      valueList.add(entry.getValue());
    }

    writeArray(keyList, keyType);
    writeArray(valueList, valueType);
  }

  @Override
  public void close() throws IOException {
    try {
      out.writeSInt64(ProtoWireConstant.TUNNEL_META_COUNT, count);
      out.writeUInt32(ProtoWireConstant.TUNNEL_META_CHECKSUM, (int) crccrc.getValue());
      out.flush();
      bou.close();
    } finally {
      if (def != null) {
        def.end();
      }
    }
  }

  /**
   * 返回已经写出的 protobuf 序列化后的字节数。
   *
   * 这个数字不包含已经存在于 buffer 中，但是尚未 flush 的内容。
   * 如果需要全部序列化过的字节数，需要在调用本方法前先调用 flush()
   *
   * @return 字节数
   */
  public long getTotalBytes() {
    return bou.getByteCount();
  }
  
  @Deprecated
  public void write(RecordPack pack) throws IOException {
    if (pack instanceof ProtobufRecordPack) {
      ProtobufRecordPack pbPack = (ProtobufRecordPack) pack;
      pbPack.checkTransConsistency(shouldTransform);
      pbPack.getProtobufStream().writeTo(bou);
      count += pbPack.getSize();
      setCheckSum(pbPack.getCheckSum());
    } else {
      RecordReader reader = pack.getRecordReader();
      Record record;
      while ((record = reader.read()) != null) {
        write(record);
      }
    }
  }

  public void flush() throws IOException {
    out.flush();
  }

  /**
   * 获取已经写出的 CheckSum
   */
  public Checksum getCheckSum() {
    return crccrc;
  }

  public void setCheckSum(Checksum checkSum) {
    crccrc = checkSum;
  }

}
