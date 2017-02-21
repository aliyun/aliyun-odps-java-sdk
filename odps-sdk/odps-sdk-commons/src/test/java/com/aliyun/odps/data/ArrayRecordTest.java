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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.PrimitiveTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

public class ArrayRecordTest {

  private static final String STRING_CHARSET = "UTF-8";

  @Test
  public void testTypeInfo() {

    TableSchema schema = new TableSchema();

    schema.addColumn(new Column("col0", TypeInfoFactory.INT));
    schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));
    schema.addColumn(new Column("col3", TypeInfoFactory.DECIMAL));
    schema.addColumn(new Column("col4", TypeInfoFactory.TINYINT));
    schema.addColumn(new Column("col5", TypeInfoFactory.SMALLINT));
    schema.addColumn(new Column("col6", TypeInfoFactory.DOUBLE));
    schema.addColumn(new Column("col7", TypeInfoFactory.FLOAT));
    schema.addColumn(new Column("col8", TypeInfoFactory.BOOLEAN));
    schema.addColumn(new Column("col9", TypeInfoFactory.DATE));
    schema.addColumn(new Column("col10", TypeInfoFactory.DATETIME));
    schema.addColumn(new Column("col11", TypeInfoFactory.TIMESTAMP));
    schema.addColumn(new Column("col12", TypeInfoFactory.VOID));
    schema.addColumn(new Column("col13", OdpsType.BINARY));
    schema.addColumn(new Column("col14", TypeInfoFactory.getVarcharTypeInfo(2)));
    schema.addColumn(new Column("col15", TypeInfoFactory.getCharTypeInfo(10)));
    schema.addColumn(new Column("col16", TypeInfoFactory.getDecimalTypeInfo(10, 2)));
    schema.addColumn(new Column("col17", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.INT)));
    schema.addColumn(new Column("col18", TypeInfoFactory
        .getMapTypeInfo(TypeInfoFactory.INT, TypeInfoFactory.STRING)));
    String[] names = {"name", "age", "weight", "parents"};
    TypeInfo[] infos =
        {TypeInfoFactory.STRING, TypeInfoFactory.INT, TypeInfoFactory.FLOAT,
         TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.INT)};
    schema.addColumn(new Column("col19", TypeInfoFactory.getStructTypeInfo(Arrays.asList(names), Arrays.asList(infos))));

    ArrayRecord record = new ArrayRecord(schema);

    record.set(0, 10);
    record.set(1, 10L);
    record.set("col2", "hello");
    record.set("col3", new BigDecimal("12.0"));
    byte a = 68;
    record.setTinyint(4, a);
    short b = 10;
    record.setSmallint(5, b);
    double d = 3.1415926;
    record.setDouble("col6", d);
    record.set(7, 3.14f);
    record.set(8, true);
    Date time = new Date();
    record.set(9, new java.sql.Date(time.getTime()));
    record.setDatetime(10, time);
    record.setTimestamp(11, new java.sql.Timestamp(time.getTime()));
    Void v = null;
    record.set("col12", v);
    record.set("col13", new Binary(new byte[]{'1', '2'}));
    record.setVarchar(14, new Varchar("he"));
    record.setChar(15, new Char("hello!"));
    record.setDecimal(16, new BigDecimal("12.23"));
    List<Integer> array = new ArrayList<Integer>();
    array.add(0, 12);
    record.set(17, array);
    Map<Integer, String> map = new HashMap<Integer, String>();
    map.put(10, "map");
    map.put(12, "map2");
    record.setMap("col18", map);
    Map<String, Integer> parents = new HashMap<String, Integer>();
    parents.put("mother", 45);
    parents.put("father", 50);
    Object [] values = {"Lily", 20, 99.8f, parents};
    Struct
        struct = new SimpleStruct((StructTypeInfo) schema.getColumn("col19").getTypeInfo(), Arrays.asList(values));
    record.setStruct(19, struct);

    Integer i = 10;
    Assert.assertEquals(i, record.getInt(0));
    Long l = 10L;
    Assert.assertEquals(l, record.getBigint(1));
    Assert.assertEquals("hello", record.getString(2));
    Assert.assertEquals(new BigDecimal("12.0"), record.getDecimal("col3"));
    Assert.assertEquals((Byte) a, record.getTinyint("col4"));
    Assert.assertEquals((Short) b, record.getSmallint(5));
    Assert.assertEquals((Double) d, record.getDouble(6));
    Assert.assertEquals((Float) 3.14f, record.getFloat("col7"));
    Assert.assertEquals(true, record.getBoolean(8));
    Assert.assertEquals(time, record.getDate(9));
    Assert.assertEquals(time, record.getDatetime(10));
    Assert.assertEquals(time, record.getTimestamp(11));
    Assert.assertEquals(null, record.get("col12"));
    Assert.assertEquals(new Binary(new byte[]{'1', '2'}), record.getBinary(13));
    Assert.assertEquals(new Varchar("he"), record.getVarchar(14));
    Assert.assertEquals("hello!", record.getChar("col15").getValue());
    Assert.assertEquals("12.23", record.getDecimal(16).toString());
    Assert.assertArrayEquals(array.toArray(), record.getArray(Integer.class, 17).toArray());
    Assert.assertTrue(map.equals(record.getMap(Integer.class, String.class, 18)));

    Struct value = record.getStruct(19);
    Assert.assertEquals("Lily", value.getFieldValue("name"));
    Assert.assertTrue(value.getFieldTypeInfo(0) instanceof PrimitiveTypeInfo);
    Assert.assertEquals(20, value.getFieldValue(1));
    Assert.assertEquals(99.8f, value.getFieldValue("weight"));
    Assert.assertTrue(value.getFieldValue("parents") instanceof Map);
    Assert.assertTrue(value.getFieldTypeInfo(3) instanceof MapTypeInfo);
    Map p = (Map) value.getFieldValue("parents");
    Assert.assertEquals(2, p.size());
    Assert.assertArrayEquals(new String[]{"mother", "father"}, p.keySet().toArray());
    Assert.assertArrayEquals(new Integer [] {45, 50}, p.values().toArray());
  }


  @Test
  public void testArrayConstructor() throws ArrayComparisonFailure, UnsupportedEncodingException {
    Column col1 = new Column("col1", OdpsType.STRING);
    Column col2 = new Column("col2", OdpsType.STRING);
    Column[] columns = new Column[]{col1, col2};
    ArrayRecord record = new ArrayRecord(columns);

    testGetBytes(record);
  }


  public void testGetBytes(ArrayRecord record)
      throws ArrayComparisonFailure, UnsupportedEncodingException {
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

    String[] strArray = {"test1", "test2",};
    record.setArray("col4", Arrays.asList(strArray));

    List list = record.getArray(Long.class, "col3");
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(list.get(0).getClass(), Long.class);
    Assert.assertEquals(2L, list.get(0));
    Assert.assertEquals(3L, list.get(1));

    try {
      record.getArray(String.class, 2);
    } catch (ClassCastException e) {
      count++;
    }

    list = record.getArray(String.class, 3);
    Assert.assertEquals(2, list.size());
    Assert.assertEquals(list.get(0).getClass(), String.class);
    Assert.assertEquals("test1", list.get(0));
    Assert.assertEquals("test2", list.get(1));

    list = record.getArray(byte[].class, "col4");
    Assert.assertEquals(list.size(), 2);
    Assert.assertEquals(list.get(0).getClass(), byte[].class);
    Assert.assertArrayEquals("test1".getBytes(), (byte[]) list.get(0));
    Assert.assertArrayEquals("test2".getBytes(), (byte[]) list.get(1));

    try {
      List<Double> res = record.getArray(Double.class, 3);
    } catch (ClassCastException e) {
      count++;
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

    Map mapRes1 = record.getMap(Long.class, String.class, "col5");
    Assert.assertEquals(Long.class, mapRes1.keySet().iterator().next().getClass());
    Assert.assertEquals(2, mapRes1.size());
    Assert.assertEquals(String.class, mapRes1.get(5L).getClass());
    Assert.assertEquals("start", mapRes1.get(5L));
    Assert.assertEquals(String.class, mapRes1.get(6L).getClass());
    Assert.assertEquals("end", mapRes1.get(6L));

    mapRes1 = record.getMap("col6");
    Assert.assertEquals(2, mapRes1.size());
    Set keySet = mapRes1.keySet();
    Assert.assertEquals(String.class, keySet.iterator().next().getClass());

    mapRes1 = record.getMap(byte[].class, Long.class, "col6");
    Assert.assertEquals(2, mapRes1.size());
    keySet = mapRes1.keySet();
    Assert.assertEquals(byte[].class, keySet.iterator().next().getClass());

    try {
      Map<String, String> mapRes2 = record.getMap(String.class, String.class, 5);
    } catch (ClassCastException e) {
      count++;
    }

    Assert.assertEquals(1, count);

  }

  @Test
  public void testSchemaConstructor() throws UnsupportedEncodingException {
    // string, string, array<bigint>, array<string>, map<bigint, string>, map<string,bigint>
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

  @Test
  public void testCastError() {

    TableSchema schema = new TableSchema();

    schema.addColumn(new Column("col0", OdpsType.INT));
    schema.addColumn(new Column("col1", OdpsType.BIGINT));
    schema.addColumn(new Column("col2", OdpsType.STRING));
    schema.addColumn(new Column("col3", OdpsType.DECIMAL));
    schema.addColumn(new Column("col4", OdpsType.TINYINT));
    schema.addColumn(new Column("col5", OdpsType.SMALLINT));
    schema.addColumn(new Column("col6", OdpsType.DOUBLE));
    schema.addColumn(new Column("col7", OdpsType.FLOAT));
    schema.addColumn(new Column("col8", OdpsType.BOOLEAN));
    schema.addColumn(new Column("col9", OdpsType.DATE));
    schema.addColumn(new Column("col10", OdpsType.DATETIME));
    schema.addColumn(new Column("col11", OdpsType.TIMESTAMP));
//    schema.addColumn(new Column("col12", OdpsType.VOID));
    schema.addColumn(new Column("col13", TypeInfoFactory.getVarcharTypeInfo(2)));
    schema.addColumn(new Column("col14", TypeInfoFactory.getCharTypeInfo(2)));
    schema.addColumn(new Column("col15", OdpsType.BINARY));
    schema.addColumn(new Column("col16", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.INT)));
    schema.addColumn(new Column("col17", TypeInfoFactory
        .getMapTypeInfo(TypeInfoFactory.INT, TypeInfoFactory.STRING)));
//    schema.addColumn(new Column("col18", OdpsType.INTERVAL_DAY_TIME));
//    schema.addColumn(new Column("col19", OdpsType.INTERVAL_YEAR_MONTH));

    ArrayRecord r = new ArrayRecord(schema);
    int count = 0;

    for (int i = 0; i < 17; i++) {
      try {
        if (i == 0) {
          r.set(i, 3L);
        } else {
          r.set(i, 2);
        }
      } catch (ClassCastException e) {
        count++;
      }
    }

    Assert.assertEquals(r.getColumnCount(), count);
  }
}
