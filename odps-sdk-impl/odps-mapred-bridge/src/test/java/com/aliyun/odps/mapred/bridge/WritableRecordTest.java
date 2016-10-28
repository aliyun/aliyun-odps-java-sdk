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

package com.aliyun.odps.mapred.bridge;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;

public class WritableRecordTest {
  
  private static final String STRING_CHARSET = "UTF-8";
  
  @Test
  public void testBinary() throws UnsupportedEncodingException {
    Record r = new WritableRecord(new Column[]{new Column("s", OdpsType.STRING)});

    String s = "Hello, World";

    // i:string o:string
    r.setString(0, s);
    assertEquals(s, r.getString(0));

    // i:string o:bytes
    assertArrayEquals(s.getBytes(), r.getBytes(0));

    // i:bytes o:string
    r.setString(0, s.getBytes());
    assertArrayEquals(s.getBytes(), r.getString(0).getBytes());

    // i:bytes o:bytes
    assertArrayEquals(s.getBytes(), r.getBytes(0));

    String sm = "你好，世界！";

    // i:string o:string
    r.setString(0, sm);
    assertEquals(sm, r.getString(0));

    // i:string o:bytes
    assertArrayEquals(sm.getBytes("UTF-8"), r.getBytes(0));

    // i:bytes o:string
    r.setString(0, sm.getBytes("UTF-8"));
    assertArrayEquals(sm.getBytes("UTF-8"), r.getString(0).getBytes("UTF-8"));

    // i:bytes o:bytes
    assertArrayEquals(sm.getBytes("UTF-8"), r.getBytes(0));
  }

  @Test
  public void testNull() {
    Column[] schema =
        new Column[]{
            new Column("col1", OdpsType.BIGINT),
            new Column("col2", OdpsType.BOOLEAN),
            new Column("col3", OdpsType.DOUBLE),
            new Column("col4", OdpsType.STRING)};
    WritableRecord r = new WritableRecord(schema);

    r.setBigint(0, null);
    r.setBoolean(1, null);
    r.setDouble(2, null);
    r.setString(3, (byte[]) null);
    r.setString(3, (String) null);
    assertNull(r.getBigint(0));
    assertNull(r.getBoolean(1));
    assertNull(r.getDouble(2));
    assertNull(r.getString(3));

    r.set(new Object[]{null, null, null, null});
    assertNull(r.get(0));
    assertNull(r.get(1));
    assertNull(r.get(2));
    assertNull(r.get(3));

    // test null clone
    WritableRecord rec = (WritableRecord) r.clone();
    assertNull(rec.get(0));
    assertNull(rec.get(1));
    assertNull(rec.get(2));
    assertNull(rec.get(3));

  }
  
  @Test
  public void testGetBytes() throws ArrayComparisonFailure, UnsupportedEncodingException {
    // positive case
    Column col1 = new Column("col1", OdpsType.STRING);
    Column col2 = new Column("col2", OdpsType.STRING);
    Column[] columns = new Column[] {col1, col2};
    WritableRecord record = new WritableRecord(columns);
    record.set("col1", "val1");
    record.set(1, "val2");
    byte[] b1 = record.getBytes(0);
    byte[] b2 = record.getBytes(1);
    Assert.assertEquals(4, b1.length);
    Assert.assertArrayEquals("Byte not equal", b1, "val1".getBytes());
    Assert.assertEquals(4, b2.length);
    Assert.assertArrayEquals("Byte not equal", b2, "val2".getBytes());

    record.set(0, "中文测试1");
    record.set("col2", "中文测试2");
    b1 = record.getBytes(0);
    b2 = record.getBytes(1);
    Assert.assertEquals(13, b1.length);
    Assert.assertArrayEquals("Byte not equal", b1, "中文测试1".getBytes(STRING_CHARSET));
    Assert.assertEquals(13, b2.length);
    Assert.assertArrayEquals("Byte not equal", b2, "中文测试2".getBytes(STRING_CHARSET));

    record.set(0, "");
    record.set("col2", null);
    b1 = record.getBytes(0);
    b2 = record.getBytes(1);
    Assert.assertEquals(0, b1.length);
    Assert.assertArrayEquals("Byte not equal", b1, "".getBytes());
    Assert.assertNull(b2);
  }

  @Test
  public void testAccessColumnStats() {
    // positive case
    Column col1 = new Column("col1", OdpsType.STRING);
    Column col2 = new Column("col2", OdpsType.STRING);
    Column[] columns = new Column[] {col1, col2};
    WritableRecord record = new WritableRecord(columns);
    record.setEnableColumnAccessStat(true);
    record.set("col1", "val1");
    record.set(1, "val2");
    byte[] b1 = record.getBytes(0);
    Assert.assertEquals(1, record.getColumnAccessedNum());

    WritableRecord record2 = (WritableRecord) record.clone();
    Assert.assertEquals(2, ((WritableRecord)record).getColumnAccessedNum());
  }
}
