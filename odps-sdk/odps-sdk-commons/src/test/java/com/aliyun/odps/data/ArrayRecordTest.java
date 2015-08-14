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

package com.aliyun.odps.data;

import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;

public class ArrayRecordTest {

  private static final String STRING_CHARSET = "UTF-8";

  @Test
  public void testGetBytes() throws ArrayComparisonFailure, UnsupportedEncodingException {
    // positive case
    Column col1 = new Column("col1", OdpsType.STRING);
    Column col2 = new Column("col2", OdpsType.STRING);
    Column[] columns = new Column[] {col1, col2};
    ArrayRecord record = new ArrayRecord(columns);
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

}
