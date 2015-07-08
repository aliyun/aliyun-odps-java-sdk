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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.List;
import java.util.zip.InflaterInputStream;

import org.xerial.snappy.SnappyFramedInputStream;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;

/**
 * @author chao.liu
 */
class ProtobufRecordStreamReader implements RecordReader {

  private ProtobufInputStream in;
  private Column[] columns;
  private long count;

  private Checksum crc = new Checksum();
  private Checksum crccrc = new Checksum();

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

    BufferedInputStream bin = new BufferedInputStream(in);

    if (option != null) {
      if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
        this.in = new ProtobufInputStream(new InflaterInputStream(bin));
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_SNAPPY)) {
        this.in = new ProtobufInputStream(new SnappyFramedInputStream(bin));
      } else if (option.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_RAW)) {
        this.in = new ProtobufInputStream(bin);
      } else {
        throw new IOException("invalid compression option.");
      }
    } else {
      this.in = new ProtobufInputStream(bin);
    }
  }

  @Override
  public Record read() throws IOException {
    Record record = new ArrayRecord(columns);
    while (true) {
      int checkSum = 0;
      int i = in.readFieldNumber();
      if (i == WireFormat.TUNNEL_END_RECORD) {
        checkSum = (int) crc.getValue();
        if (in.readUInt32() != checkSum) {
          throw new IOException("Checksum invalid.");
        }
        crc.reset();
        crccrc.update(checkSum);
        break;
      }
      if (i == WireFormat.TUNNEL_META_COUNT) {
        if (count != in.readLong()) {
          throw new IOException("count does not match.");
        }

        if (WireFormat.TUNNEL_META_CHECKSUM != in.readFieldNumber()) {
          throw new IOException("Invalid stream.");
        }

        if ((int) crccrc.getValue() != in.readUInt32()) {
          throw new IOException("Checksum invalid.");
        }

        if (in.read() >= 0) {
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
          record.setDouble(i - 1, v);
          break;
        }
        case BOOLEAN: {
          boolean v = in.readBoolean();
          crc.update(v);
          record.setBoolean(i - 1, v);
          break;
        }
        case BIGINT: {
          long v = in.readLong();
          crc.update(v);
          record.setBigint(i - 1, v);
          break;
        }
        case STRING: {
          byte[] bytes = in.readRawBytes();
          crc.update(bytes, 0, bytes.length);
          record.setString(i - 1, bytes);
          break;
        }
        case DATETIME: {
          long v = in.readLong();
          crc.update(v);
          record.setDatetime(i - 1, DateUtils.ms2date(v));
          break;
        }
        case DECIMAL: {
          byte[] bytes = in.readRawBytes();
          crc.update(bytes, 0, bytes.length);
          BigDecimal decimal = new BigDecimal(new String(bytes, "UTF-8"));
          record.setDecimal(i - 1, decimal);
          break;
        }
        default:
          throw new IOException("Unsupported type " + columns[i - 1].getType());
      }
    }
    count++;
    return record;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  public long getTotalBytes() {
    return in.getTotalBytes();
  }
}
