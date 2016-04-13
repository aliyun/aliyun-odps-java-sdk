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

package com.aliyun.odps.datahub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamWriter;
import com.aliyun.odps.data.Record;


public class DatahubRecordPack {

  private ByteArrayOutputStream byteArrayOutputStream;
  private ProtobufRecordStreamWriter protobufRecordStreamWriter;
  private TableSchema recordSchema;
  private long recordCount;
  private int maxPackSize = 1024 * 1024 * 8;
  private boolean packSealed = false;

  /**
   * 新建一个DatahubRecordPack
   *
   * @param recordSchema
   * @throws IOException 
   */
  public DatahubRecordPack(TableSchema recordSchema) throws IOException {
    this.recordSchema = recordSchema;
    this.byteArrayOutputStream = new ByteArrayOutputStream();
    this.recordCount = 0;
    this.protobufRecordStreamWriter =
        new ProtobufRecordStreamWriter(recordSchema, byteArrayOutputStream);
  }

  /**
   * 向DatahubRecordPack中append一条Record。
   *
   * @param r
   * @throws IOException 产生异常表示需要通过DatahubWriter将DatahubRecordPack中的数据发送到datahub
   */
  public void append(Record r) throws IOException {
    if (packSealed == true) {
      throw new IOException("Append record to a sealed pack. Please use Clear() to clear this pack or new another pack.");
    }
    if (protobufRecordStreamWriter == null) {
      protobufRecordStreamWriter =
          new ProtobufRecordStreamWriter(recordSchema, byteArrayOutputStream);
    }
    if (protobufRecordStreamWriter.getTotalBytes() >= this.maxPackSize) {
      throw new IOException("Pack reach max size. Please send this pack and create new pack to append.");
    }
    protobufRecordStreamWriter.write(r);
    recordCount += 1;
  }

  /**
   * 清空DatahubRecordPack
   */
  public void clear() {
    try {
      if (protobufRecordStreamWriter != null) {
        protobufRecordStreamWriter.close();
      }
    } catch (IOException e) {

    }

    protobufRecordStreamWriter = null;
    byteArrayOutputStream.reset();
    recordCount = 0;
    packSealed = false;
  }

  public byte[] getByteArray() throws IOException {
    packSealed = true;
    if (protobufRecordStreamWriter != null) {
      protobufRecordStreamWriter.close();
      protobufRecordStreamWriter = null;
    }
    return byteArrayOutputStream.toByteArray();
  }

  public long getRecordCount() {
    return recordCount;
  }
}
