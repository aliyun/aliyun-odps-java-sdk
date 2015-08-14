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
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.zip.Deflater;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;

public class TableTunnelTest extends TestBase {

  private static TableSchema schema;
  static String tableName = "table_name_for_test";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    odps = OdpsTestUtils.newDefaultOdps();
    // odps = OdpsTestUtils.createOdpsByTaobaoAccount();

    if (odps.tables().exists(tableName)) {
      odps.tables().delete(tableName);
    }

    schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addColumn(new Column("c2", OdpsType.DOUBLE));
    schema.addColumn(new Column("c3", OdpsType.BOOLEAN));
    schema.addColumn(new Column("c4", OdpsType.DATETIME));
    schema.addColumn(new Column("c5", OdpsType.STRING));
    schema.addColumn(new Column("c6", OdpsType.DECIMAL));

    schema.addPartitionColumn(new Column("p1", OdpsType.BIGINT));
    schema.addPartitionColumn(new Column("p2", OdpsType.STRING));

    odps.tables().create(odps.getDefaultProject(), tableName, schema);

    Table table = odps.tables().get(tableName);
    table.createPartition(new PartitionSpec("p1=1,p2='A'"));
    table.createPartition(new PartitionSpec("p1=1,p2='DEFLATER'"));
    testUpload();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    odps.tables().delete(tableName);
  }

  public static void testUpload() throws TunnelException, IOException {
    TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
    TableTunnel.UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(),
                                                                   tableName, new PartitionSpec(
        "p1=1,p2='A'"));
    TunnelRecordWriter rw = (TunnelRecordWriter) session.openRecordWriter(0L);
    Record record = session.newRecord();
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < 100; i++) {
      record.setBigint("c1", Long.parseLong(random.nextInt(1000) + ""));
      record.setDouble("c2", random.nextDouble());
      record.setBoolean("c3", random.nextBoolean());
      record.setDatetime("c4", new Date());
      record.setString("c5", "str_" + random.nextInt(100));
      record.setDecimal("c6", new BigDecimal("1.23"));

      rw.write(record);
    }
    byte[] byteString = new byte[500];
    for (int i = 0; i < 100; i++) {
      record.setBigint("c1", Long.parseLong(random.nextInt(1000) + ""));
      record.setDouble("c2", random.nextDouble());
      record.setBoolean("c3", random.nextBoolean());
      record.setDatetime("c4", new Date());
      random.nextBytes(byteString);
      record.setString("c5", byteString);
      record.setDecimal("c6", new BigDecimal("1.23"));
      rw.write(record);
      rw.flush();
      rw.flush();
    }
    rw.close();
    Long[] blocks = {0L};
    session.commit(blocks);
  }

  @Test(expected = NullPointerException.class)
  public void testDefalterClose() throws Exception {
    TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
    TableTunnel.UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(),
                                                                   tableName, new PartitionSpec(
        "p1=1,p2='DEFLATER'"));
    RecordWriter rw = session.openRecordWriter(0L, true);
    Record record = session.newRecord();
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < 100; i++) {
      record.setBigint("c1", Long.parseLong(random.nextInt(1000) + ""));
      record.setDouble("c2", random.nextDouble());
      record.setBoolean("c3", random.nextBoolean());
      record.setDatetime("c4", new Date());
      record.setString("c5", "str_" + random.nextInt(100));
      rw.write(record);
    }
    byte[] byteString = new byte[500];
    for (int i = 0; i < 100; i++) {
      record.setBigint("c1", Long.parseLong(random.nextInt(1000) + ""));
      record.setDouble("c2", random.nextDouble());
      record.setBoolean("c3", random.nextBoolean());
      record.setDatetime("c4", new Date());
      random.nextBytes(byteString);
      record.setString("c5", byteString);
      rw.write(record);
    }
    rw.close();

    Long[] blocks = {0L};
    session.commit(blocks);

    Field field = rw.getClass().getSuperclass().getDeclaredField("def");
    field.setAccessible(true);
    Deflater def = (Deflater) field.get(rw);
    def.getAdler();
  }

  @Test
  public void testDownload() throws TunnelException, IOException {
    TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
    TableTunnel.DownloadSession session = tunnel.createDownloadSession(odps.getDefaultProject(),
                                                                       tableName, new PartitionSpec(
        "p1=1,p2='A'"));

    System.out.println(session.getRecordCount());

    TableSchema schema1 = session.getSchema();
    for (int i = 0; i < schema1.getColumns().size(); i++) {
      Column col = schema1.getColumn(i);
      System.out.print(col.getName() + "\t");
    }
    System.out.println();

    RecordReader reader = session.openRecordReader(0, session.getRecordCount());
    Record record;
    while ((record = reader.read()) != null) {
      System.out.print(record.get("c1") + "\t");
      System.out.print(record.get("c2") + "\t");
      System.out.print(record.get("c3") + "\t");
      System.out.print(record.get("c4") + "\t");
      System.out.print("" + ((byte[]) record.get("c5")).length + "\t");
      System.out.print("" + ((byte[]) record.getBytes("c5")).length + "\t");
      System.out.print(record.get("c6") + "\t");
      System.out.println();
    }

    reader.close();
  }

  @Test
  public void testDownloadReuse() throws TunnelException, IOException {
    TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
    TableTunnel.DownloadSession session = tunnel.createDownloadSession(odps.getDefaultProject(),
                                                                       tableName, new PartitionSpec(
            "p1=1,p2='A'"));

    System.out.println(session.getRecordCount());

    TableSchema schema1 = session.getSchema();
    for (int i = 0; i < schema1.getColumns().size(); i++) {
      Column col = schema1.getColumn(i);
      System.out.print(col.getName() + "\t");
    }
    System.out.println();

    TunnelRecordReader reader = session.openRecordReader(0, session.getRecordCount());
    Record record = null;
    record = reader.read(record);
    assertTrue(record == reader.read(record));
    while ((record = reader.read()) != null) {
      System.out.print(record.get("c1") + "\t");
      System.out.print(record.get("c2") + "\t");
      System.out.print(record.get("c3") + "\t");
      System.out.print(record.get("c4") + "\t");
      System.out.print("" + ((byte[]) record.get("c5")).length + "\t");
      System.out.print("" + ((byte[]) record.getBytes("c5")).length + "\t");
      System.out.print(record.get("c6") + "\t");
      System.out.println();
    }

    reader.close();
  }

  @Test
  public void testDownloadColumn() throws TunnelException, IOException {
    TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
    ArrayList<Column> columns = new ArrayList<Column>();
    columns.add(new Column("c1", OdpsType.BIGINT));
    TableTunnel.DownloadSession session = tunnel.createDownloadSession(odps.getDefaultProject(),
                                                                       tableName, new PartitionSpec(
        "p1=1,p2='A'"));

    RecordReader reader = session.openRecordReader(0, session.getRecordCount(), false, columns);
    Record record;
    while ((record = reader.read()) != null) {
      System.out.print(record.get("c1"));
      assertEquals(1, record.getColumnCount());
      System.out.println();
    }

    reader.close();
  }

}
