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

package com.aliyun.odps.tunnel.io;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Date;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import org.apache.commons.io.output.CountingOutputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordPack;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.InvalidColumnTypeException;
import com.google.protobuf.CodedOutputStream;

/**
 * @author chao.liu
 */
class ProtobufRecordStreamWriter implements RecordWriter {

  private CountingOutputStream bou;
  private Column[] columns;
  private CodedOutputStream out;
  private long count;

  private Checksum crc = new Checksum();
  private Checksum crccrc = new Checksum();
  private Deflater def;

  public ProtobufRecordStreamWriter(TableSchema schema, OutputStream out) throws IOException {
    this(schema, out, null);
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

  static void writeRawBytes(int fieldNumber, byte[] value, CodedOutputStream out) throws IOException {
    out.writeTag(fieldNumber, com.google.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeRawVarint32(value.length);
    out.writeRawBytes(value);
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

      OdpsType type = columns[i].getType();
      switch (type) {
        case BOOLEAN: {
          boolean value = (Boolean) v;
          crc.update(value);
          out.writeBool(pbIdx, value);
          break;
        }
        case DATETIME: {
          Date value = (Date) v;
          Long longValue = DateUtils.date2ms(value);
          crc.update(longValue);
          out.writeSInt64(pbIdx, longValue);
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
          writeRawBytes(pbIdx, bytes, out);
          break;
        }
        case DOUBLE: {
          double value = (Double) v;
          crc.update(value);
          out.writeDouble(pbIdx, value);
          break;
        }
        case BIGINT: {
          long value = (Long) v;
          crc.update(value);
          out.writeSInt64(pbIdx, value);
          break;
        }
        case DECIMAL: {
          String value = ((BigDecimal) v).toPlainString();
          byte[] bytes = value.getBytes("UTF-8");
          crc.update(bytes, 0, bytes.length);
          writeRawBytes(pbIdx, bytes, out);
          break;
        }
        default:
          throw new IOException(new InvalidColumnTypeException("Invalid data type: " + type));
      }
    }

    int checksum = (int) crc.getValue();
    out.writeUInt32(TunnelWireConstant.TUNNEL_END_RECORD, checksum);

    crc.reset();
    crccrc.update(checksum);

    count++;
  }

  @Override
  public void close() throws IOException {

    try {
      out.writeSInt64(TunnelWireConstant.TUNNEL_META_COUNT, count);
      out.writeUInt32(TunnelWireConstant.TUNNEL_META_CHECKSUM, (int) crccrc.getValue());
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

  public void write(RecordPack pack) throws IOException {
    if (pack instanceof ProtobufRecordPack) {
      ProtobufRecordPack pbPack = (ProtobufRecordPack) pack;
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

  void setCheckSum(Checksum checkSum) {
    crccrc = checkSum;
  }

}
