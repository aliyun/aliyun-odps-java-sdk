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

package com.aliyun.odps;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;

public class DecimalTest extends TestBase {

  private final static String tableName = "decimal_test";

  @Before
  public void setup() throws OdpsException, IOException {
    try {
      odps.tables().delete(tableName);
    } catch (OdpsException e) {

    }

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c0", OdpsType.DECIMAL));

    odps.tables().create(tableName, schema);
  }

  @Test
  public void testDecimal() throws IOException, OdpsException {

    TableTunnel tunnel = OdpsTestUtils.newTableTunnel(odps);
    UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(), tableName);
    Record record = session.newRecord();
    RecordWriter writer = session.openRecordWriter(0);

    BigDecimal decimal = new BigDecimal("1.23");

    record.setDecimal(0, decimal);
    writer.write(record);
    writer.close();
    session.commit(session.getBlockList());

    Table table = odps.tables().get(tableName);
    RecordReader reader = table.read(10000);
    Record record2 = reader.read();
    assertEquals(record2.getDecimal(0).stripTrailingZeros(), decimal);
  }
}
