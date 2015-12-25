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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;

public class ArrayRecordTest {

  private static final String STRING_CHARSET = "UTF-8";


  @Test
  public void testArrayConstructor() throws ArrayComparisonFailure, UnsupportedEncodingException {
    Column col1 = new Column("col1", OdpsType.STRING);
    Column col2 = new Column("col2", OdpsType.STRING);
    Column[] columns = new Column[]{col1, col2};
    ArrayRecord record = new ArrayRecord(columns);

    testGetBytes(record);
  }


  public void testGetBytes(ArrayRecord record) throws ArrayComparisonFailure, UnsupportedEncodingException {
    // positive case
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

  public void testGetArray(ArrayRecord record) {
    int count = 0;
    Long[] array = new Long[2];
    array[0] = 2L;
    array[1] = 3L;
    record.setArray(2, Arrays.asList(array));

    String [] strArray = {"test1", "test2",};
    record.setArray("col4", Arrays.asList(strArray));

    List list = record.getArray(Long.class, "col3");
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(list.get(0).getClass(), Long.class);
    Assert.assertEquals(2L, list.get(0));
    Assert.assertEquals(3L, list.get(1));

    try {
      record.getArray(String.class, 2);
    } catch (ClassCastException e) {
      count ++;
    }

    list = record.getArray(String.class, 3);
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(list.get(0).getClass(), String.class);
    Assert.assertEquals("test1", list.get(0));
    Assert.assertEquals("test2", list.get(1));

    list = record.getArray("col4");
    Assert.assertEquals(list.size(), 2);
    Assert.assertEquals(list.get(0).getClass(), byte[].class);
    Assert.assertArrayEquals("test1".getBytes(), (byte [])list.get(0));
    Assert.assertArrayEquals("test2".getBytes(), (byte []) list.get(1));

    try {
      List<Double> res = record.getArray(Double.class, 3);
      Double d = res.get(0);
    } catch (ClassCastException e) {
      count ++;
    }

    Assert.assertEquals(2, count);
  }

  public void testGetMap(ArrayRecord record) {
    int count = 0;

    Map map = new HashMap<Long, String>();
    map.put(5L, "start");
    map.put(6L, "end");
    record.setMap("col5", map);

    map = new HashMap<String, Long>();
    map.put("start", 7L);
    map.put("end", 8L);
    record.setMap("col6", map);

    Map mapRes1 = record.getMap(long.class, String.class, "col5");
    Assert.assertEquals(2, mapRes1.size());
    Assert.assertEquals(String.class, mapRes1.get(5L).getClass());
    Assert.assertEquals("start", mapRes1.get(5L));
    Assert.assertEquals(String.class, mapRes1.get(6L).getClass());
    Assert.assertEquals("end", mapRes1.get(6L));

    mapRes1 = record.getMap("col6");
    Assert.assertEquals(2, mapRes1.size());
    Set keySet = mapRes1.keySet();
    Assert.assertEquals(byte[].class, keySet.iterator().next().getClass());

    try {
      Map<String, String> mapRes2 = record.getMap(String.class, String.class, 5);
    } catch (ClassCastException e) {
      count++;
    }

    Assert.assertEquals(1, count);

  }

  @Test
  public void testSchemaConstructor() throws ArrayComparisonFailure, UnsupportedEncodingException {
    TableSchema schema = new TableSchema();

    schema.addColumn(new Column("col1", OdpsType.STRING));
    schema.addColumn(new Column("col2", OdpsType.STRING));

    List<OdpsType> arrayTypeList = new ArrayList<OdpsType>();
    arrayTypeList.add(OdpsType.BIGINT);
    Column arrayCol = new Column("col3", OdpsType.ARRAY);
    arrayCol.setGenericTypeList(arrayTypeList);
    schema.addColumn(arrayCol);

    arrayTypeList = new ArrayList<OdpsType>();
    arrayTypeList.add(OdpsType.STRING);
    arrayCol = new Column("col4", OdpsType.ARRAY);
    arrayCol.setGenericTypeList(arrayTypeList);
    schema.addColumn(arrayCol);

    List<OdpsType> mapTypeList = new ArrayList<OdpsType>();
    mapTypeList.add(OdpsType.BIGINT);
    mapTypeList.add(OdpsType.STRING);
    Column mapCol = new Column("col5", OdpsType.MAP);
    mapCol.setGenericTypeList(mapTypeList);
    schema.addColumn(mapCol);

    mapTypeList = new ArrayList<OdpsType>();
    mapTypeList.add(OdpsType.STRING);
    mapTypeList.add(OdpsType.BIGINT);
    mapCol = new Column("col6", OdpsType.MAP);
    mapCol.setGenericTypeList(mapTypeList);
    schema.addColumn(mapCol);

    ArrayRecord record = new ArrayRecord(schema);

    testGetBytes(record);
    testGetArray(record);
    testGetMap(record);
  }

  @Test
  public void testClear() {
    TableSchema schema = new TableSchema();

    schema.addColumn(new Column("col1", OdpsType.BIGINT));
    schema.addColumn(new Column("col2", OdpsType.STRING));

    ArrayRecord r = new ArrayRecord(schema);
    r.setBigint(0, 1L);
    r.setString(1, "hello");

    r.clear();

    Assert.assertNull("column 0 should be null after clear", r.get(0));
    Assert.assertNull("column 1 should be null after clear", r.get(1));
  }
}
