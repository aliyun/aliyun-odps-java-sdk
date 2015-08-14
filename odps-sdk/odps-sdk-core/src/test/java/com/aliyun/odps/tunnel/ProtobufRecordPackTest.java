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

package com.aliyun.odps.tunnel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;

public class ProtobufRecordPackTest extends TestBase {

  private static final String UPLOAD_PACK = "upload_pack";
  private static final String RESET_PACK = "reset_pack";
  private static TableSchema schema;
  private static ProtobufRecordPack pack;

  @BeforeClass
  public static void setup() throws OdpsException, IOException {
    schema = new TableSchema();
    schema.addColumn(new Column("c0", OdpsType.STRING));
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    pack = new ProtobufRecordPack(schema);
    Record record = new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    record.setString(0, "upload_pack");
    record.setBigint(1, 2L);
    pack.append(record);
    record.setString(0, "upload_pack1");
    record.setBigint(1, -1L);
    pack.append(record);

  }

  @Test(expected = UnsupportedOperationException.class)
  public void testPackRead() throws IOException {
    RecordReader reader = pack.getRecordReader();
  }

  @Test
  public void testPackUpload() throws OdpsException, IOException {
    try {
      odps.tables().delete(UPLOAD_PACK);
    } catch (Exception e) {
      // do nothing
    }
    odps.tables().create(UPLOAD_PACK, schema);
    TableTunnel tableTunnel = OdpsTestUtils.newTableTunnel(odps);
    UploadSession session = tableTunnel.createUploadSession(odps.getDefaultProject(), UPLOAD_PACK);
    TunnelRecordWriter writerCompressed = (TunnelRecordWriter) session.openRecordWriter(1, true);
    writerCompressed.write(pack);
    writerCompressed.close();

    TunnelRecordWriter writer = (TunnelRecordWriter) session.openRecordWriter(0);
    writer.write(pack);
    writer.close();
    session.commit(new Long[]{0L, 1L});
    DownloadSession
        session2 =
        tableTunnel.createDownloadSession(odps.getDefaultProject(), UPLOAD_PACK);
    RecordReader reader = session2.openRecordReader(0, 4);
    Record record;
    long cnt = 0;
    while ((record = reader.read()) != null) {
      assertTrue(record.getString(0).startsWith("upload_pack"));
      cnt += record.getBigint(1);
    }
    assertEquals(cnt, 2);

  }

  @Test
  public void testResetPack() throws OdpsException, IOException {
    try {
      odps.tables().delete(RESET_PACK);
    } catch (Exception e) {
      // do nothing
    }
    odps.tables().create(RESET_PACK, schema);
    TableTunnel tableTunnel = OdpsTestUtils.newTableTunnel(odps);
    UploadSession session = tableTunnel.createUploadSession(odps.getDefaultProject(), RESET_PACK);
    TunnelRecordWriter writerCompressed = (TunnelRecordWriter) session.openRecordWriter(1, true);
    ProtobufRecordPack pack = new ProtobufRecordPack(schema);
    Record record = new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    record.setString(0, "reset_pack");
    record.setBigint(1, 2L);
    pack.append(record);
    record.setString(0, "reset_pack1");
    record.setBigint(1, -1L);
    pack.append(record);
    pack.reset();
    record.setString(0, "reset_pack");
    record.setBigint(1, 2L);
    pack.append(record);
    record.setString(0, "reset_pack1");
    record.setBigint(1, -1L);
    pack.append(record);
    writerCompressed.write(pack);
    writerCompressed.close();
    assertEquals(pack.getTotalBytes(), 47L);
    session.commit(new Long[]{1L});

    DownloadSession
        session2 =
        tableTunnel.createDownloadSession(odps.getDefaultProject(), RESET_PACK);
    RecordReader reader = session2.openRecordReader(0, 4);
    long cnt = 0;
    while ((record = reader.read()) != null) {
      cnt += 1;
    }
    assertEquals(cnt, 2);
  }

}
