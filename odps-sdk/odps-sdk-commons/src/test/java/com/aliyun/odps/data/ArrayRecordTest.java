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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.IsoEra;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

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

  private static final Calendar GMT_CALENDER = new Calendar.Builder()
      .setCalendarType("iso8601")
      .setLenient(true)
      .setTimeZone(TimeZone.getTimeZone("GMT"))
      .build();

  private static final Calendar
      SHANGHAI_CALENDER =
      new Calendar.Builder().setCalendarType("iso8601").setLenient(true)
          .setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
          .build();

  private static final List<LocalDate> LOCAL_DATES;
  private static final List<java.sql.Date> DATES;
  static {
    List<LocalDate> localDates = new ArrayList<>(366 * 10000);
    LocalDate localDate = LocalDate.of(0, Month.JANUARY, 1);
    while (localDate.getYear() < 10000) {
      localDates.add(localDate);
      localDate = localDate.plus(1, ChronoUnit.DAYS);
    }
    LOCAL_DATES = Collections.unmodifiableList(localDates);

    List<java.sql.Date> dates = new ArrayList<>(366 * 10000);
    Calendar cal = (Calendar) GMT_CALENDER.clone();
    cal.set(0, Calendar.JANUARY, 1);
    while (cal.get(Calendar.YEAR) < 10000) {
      dates.add(new java.sql.Date(cal.getTime().getTime()));
      cal.add(Calendar.DATE, 1);
    }
    DATES = Collections.unmodifiableList(dates);
  }

  private static final Collection<String[]>
      notSameDateTimes = Arrays.asList(new String[]{"0001-01-01 00:00:00", "0001-01-01 00:05:43"},
                                        new String[]{"0011-11-03 00:00:00", "0011-11-03 00:05:43"},
                                        new String[]{"1900-01-01 00:00:00", "1900-01-01 00:05:43"});

  private static final List<String> sameDateTimes =
      Arrays.asList(
                    "1927-12-31 23:54:07",
                    "1928-01-01 00:00:00",
                    "1939-08-01 22:22:22",
                    "1986-05-04 00:00:00",
                    "1992-01-01 00:00:30",
                    "9988-09-27 09:09:09",
                    "1970-01-01 00:00:00",
                    "1970-01-01 08:00:00",
                    "9999-12-31 23:59:59");

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
    schema.addColumn(new Column("col13", TypeInfoFactory.BINARY));
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
    record.set("col12", null);
    record.set("col13", new Binary(new byte[]{'1', '2'}));
    record.setVarchar(14, new Varchar("he"));
    record.setChar(15, new Char("hello!"));
    record.setDecimal(16, new BigDecimal("12.23"));
    List<Integer> array = new ArrayList<>();
    array.add(0, 12);
    record.set(17, array);
    Map<Integer, String> map = new HashMap<>();
    map.put(10, "map");
    map.put(12, "map2");
    record.setMap("col18", map);
    Map<String, Integer> parents = new HashMap<>();
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
    Assert.assertTrue(isSameDate(new java.sql.Date(time.getTime()), GMT_CALENDER, record.getDate(9), GMT_CALENDER));
    Assert.assertEquals(time, record.getDatetime(10));
    Assert.assertEquals(time, record.getTimestamp(11));
    Assert.assertNull(record.get("col12"));
    Assert.assertEquals(new Binary(new byte[]{'1', '2'}), record.getBinary(13));
    Assert.assertEquals(new Varchar("he"), record.getVarchar(14));
    Assert.assertEquals("hello!", record.getChar("col15").getValue());
    Assert.assertEquals("12.23", record.getDecimal(16).toString());
    Assert.assertArrayEquals(array.toArray(), record.getArray(Integer.class, 17).toArray());
    Assert.assertEquals(map, record.getMap(Integer.class, String.class, 18));

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

    Assert.assertEquals(value.toString(), "{name:Lily, age:20, weight:99.8, parents:{mother=45, father=50}}");

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

    schema.addColumn(new Column("col1", TypeInfoFactory.STRING));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));

    Column arrayCol =
        new Column("col3", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT));
    schema.addColumn(arrayCol);

    arrayCol = new Column("col4", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING));
    schema.addColumn(arrayCol);

    Column mapCol = new Column("col5", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.STRING));
    schema.addColumn(mapCol);

    mapCol = new Column("col6", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.BIGINT));
    schema.addColumn(mapCol);

    ArrayRecord record = new ArrayRecord(schema);

    testGetBytes(record);
    testGetArray(record);
    testGetMap(record);
  }

  @Test
  public void testClear() {
    TableSchema schema = new TableSchema();

    schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));

    ArrayRecord r = new ArrayRecord(schema);
    r.setBigint(0, 1L);
    r.setString(1, "hello");

    r.clear();

    Assert.assertNull("column 0 should be null after clear", r.get(0));
    Assert.assertNull("column 1 should be null after clear", r.get(1));
  }

  @Test
  public void testInterval() {
    TableSchema schema = new TableSchema();

    schema.addColumn(new Column("c1", TypeInfoFactory.INTERVAL_DAY_TIME));
    schema.addColumn(new Column("c2", TypeInfoFactory.INTERVAL_DAY_TIME));
    schema.addColumn(new Column("c3", TypeInfoFactory.INTERVAL_YEAR_MONTH));
    schema.addColumn(new Column("c4", TypeInfoFactory.INTERVAL_YEAR_MONTH));

    ArrayRecord record = new ArrayRecord(schema);

    java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());

    IntervalDayTime dayTime = new IntervalDayTime(timestamp.getTime() / 1000, timestamp.getNanos());
    record.set(0, dayTime);
    record.set("c2", dayTime);

    IntervalYearMonth yearMonth = new IntervalYearMonth(16);
    record.set(2, yearMonth);
    record.set("c4", yearMonth);

    Assert.assertEquals(dayTime, record.getIntervalDayTime("c1"));
    Assert.assertEquals(dayTime, record.getIntervalDayTime(1));
    Assert.assertEquals(dayTime, record.get("c2"));
    Assert.assertEquals(dayTime, record.get(0));

    Assert.assertEquals(yearMonth, record.getIntervalYearMonth("c3"));
    Assert.assertEquals(yearMonth, record.getIntervalYearMonth(3));
    Assert.assertEquals(yearMonth, record.get("c4"));
    Assert.assertEquals(yearMonth, record.get(2));
  }

  @Test
  public void testCastError() {

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
//    schema.addColumn(new Column("col12", OdpsType.VOID));
    schema.addColumn(new Column("col13", TypeInfoFactory.getVarcharTypeInfo(2)));
    schema.addColumn(new Column("col14", TypeInfoFactory.getCharTypeInfo(2)));
    schema.addColumn(new Column("col15", TypeInfoFactory.BINARY));
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
      } catch (IllegalArgumentException e) {
        System.out.println(e.getMessage());
        count++;
      }
    }

    Assert.assertEquals(r.getColumnCount(), count);
  }

  @Test
  public void testByteAndString() {
    TableSchema schema = new TableSchema();

    schema.addColumn(new Column("col1", TypeInfoFactory.STRING));
    schema.addColumn(new Column("col2", TypeInfoFactory.STRING));


    ArrayRecord r = new ArrayRecord(schema);
    byte [] v = new byte[] {'1', '2', '3'};
    r.setString(0, v);
    r.set(1, v);

    Assert.assertTrue(r.get(0) instanceof  byte []);
    Assert.assertTrue(r.get(1) instanceof byte []);

    Assert.assertArrayEquals(new byte[]{'1', '2', '3'}, (byte [])r.get(0));
    Assert.assertArrayEquals(new byte[]{'1', '2', '3'}, (byte [])r.get(1));
    Assert.assertArrayEquals(new byte[]{'1', '2', '3'}, r.getBytes(0));
    Assert.assertEquals(new String(new byte[]{'1', '2', '3'}), r.getString(0));
    Assert.assertEquals(new String(new byte[]{'1', '2', '3'}), r.getString(1));

  }

  // TODO: ArrayRecord#getDateAsLocalDate & ArrayRecord#getDateAsLocalDate are not public yet
//  @Test
//  public void testDateAsLocalDate() {
//    TableSchema schema = new TableSchema();
//    schema.addColumn(new Column("col1", TypeInfoFactory.DATE));
//    ArrayRecord r = new ArrayRecord(schema);
//
//    for (LocalDate localDate : LOCAL_DATES) {
//      r.setDateAsLocalDate(0, localDate);
//      Assert.assertEquals(localDate, r.getDateAsLocalDate(0));
//    }
//  }

  @Test
  public void testDate() {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.DATE));
    ArrayRecord r = new ArrayRecord(schema);

    for (java.sql.Date date : DATES) {
      r.setDate(0, date);
      Assert.assertEquals(date, r.getDate(0));
    }
  }

  @Test
  public void testDateInOtherTimeZone() {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.DATE));
    ArrayRecord r = new ArrayRecord(schema);

    Calendar cal = new Calendar.Builder()
        .setCalendarType("iso8601")
        .setLenient(true)
        .setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
        .setDate(0, Calendar.JANUARY, 1)
        .setTimeOfDay(0, 0, 0)
        .build();

    while (cal.get(Calendar.YEAR) < 10000) {
      java.sql.Date date = new java.sql.Date(cal.getTime().getTime());
      r.setDate(0, date, cal);
      // Cannot ensure exactly the same timestamp in Asia/Shanghai because of the daylight saving
      // starts from 1940-06-04
      Assert.assertTrue(
          date.getTime() + " : " + r.getDate(0, cal).getTime()
          , isSameDate(date, cal, r.getDate(0, cal), cal));
      cal.add(Calendar.DATE, 1);
    }
  }

  private boolean isSameDate(
      java.sql.Date date1,
      Calendar calendar1,
      java.sql.Date date2,
      Calendar calendar2) {
    if (date1 == null && date2 == null) {
      return true;
    }
    if (date1 == null || date2 == null) {
      return false;
    }
    calendar1 = (Calendar) calendar1.clone();
    calendar1.clear();
    calendar1.setTime(date1);
    calendar2 = (Calendar) calendar2.clone();
    calendar2.clear();
    calendar2.setTime(date2);
    boolean equal = true;
    if (calendar1.get(Calendar.ERA) != calendar2.get(Calendar.ERA)) {
      equal = false;
    }
    if (calendar1.get(Calendar.YEAR) != calendar2.get(Calendar.YEAR)) {
      equal = false;
    }
    if (calendar1.get(Calendar.MONTH) != calendar2.get(Calendar.MONTH)) {
      equal = false;
    }
    if (calendar1.get(Calendar.DAY_OF_MONTH) != calendar2.get(Calendar.DAY_OF_MONTH)) {
      equal = false;
    }
    if (!equal) {
      System.out.println("c1: " + calendar1.get(Calendar.ERA) + " " + calendar1.get(Calendar.YEAR) + " " + calendar1.get(Calendar.MONTH) + " " + calendar1.get(Calendar.DAY_OF_MONTH));
      System.out.println("c1: " + calendar2.get(Calendar.ERA) + " " + calendar2.get(Calendar.YEAR) + " " + calendar2.get(Calendar.MONTH) + " " + calendar2.get(Calendar.DAY_OF_MONTH));
    }
    return equal;
  }

  @Test
  public void testDateZonedDatetimeTransfer() throws Exception {
//    testDateToZonedDateTime(GMT_CALENDER);
    testDateToZonedDateTime(SHANGHAI_CALENDER);
  }

  public void testDateToZonedDateTime(Calendar cal) throws Exception {
    Calendar calendar = (Calendar) cal.clone();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(calendar.getTimeZone().toZoneId());

    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormatter.setCalendar(calendar);

    for (String dateStr : sameDateTimes) {
      java.util.Date date = dateFormatter.parse(dateStr);
      ZonedDateTime dateTime = dateToZonedDateTime(date);
      Assert.assertEquals(dateTime, ZonedDateTime.parse(dateStr, formatter));
      if ("1986-05-04 00:00:00".equals(dateStr)) {
        String formatStr = formatter.format(dateTime);
        Assert.assertTrue(dateStr.equals(formatStr) || "1986-05-04 01:00:00".equals(formatStr));
      } else {
        Assert.assertEquals(dateStr, formatter.format(dateTime));
      }
      Assert.assertEquals(date, zonedDateTimeToDate(dateTime));
    }

    for (String[] pair : notSameDateTimes) {
      java.util.Date date = dateFormatter.parse(pair[0]);
      ZonedDateTime dateTime = dateToZonedDateTime(date);
      dateTime.toEpochSecond();
      Assert.assertNotEquals(dateTime, ZonedDateTime.parse(pair[0], formatter));
      Assert.assertEquals(pair[1], formatter.format(dateTime));
      Assert.assertEquals(date, zonedDateTimeToDate(dateTime));
    }
  }

  private Date zonedDateTimeToDate(ZonedDateTime value) {
    return Date.from(value.toInstant());
  }

  private ZonedDateTime dateToZonedDateTime(Date date) {
    return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault());
  }

  @Test
  public void testDateToLocalDate() {
    Calendar calendar = (Calendar) GMT_CALENDER.clone();
    for (java.sql.Date date : DATES) {
      LocalDate localDate = OdpsTypeTransformer.dateToLocalDate(date, GMT_CALENDER);
      calendar.clear();
      calendar.setTime(date);

      Assert.assertEquals(calendar.get(Calendar.ERA), localDate.getEra().getValue());
      boolean isBc = calendar.get(Calendar.ERA) == GregorianCalendar.BC;
      Assert.assertEquals(isBc ? 0 : calendar.get(Calendar.YEAR), localDate.getYear());
      Assert.assertEquals(calendar.get(Calendar.MONTH) + 1, localDate.getMonthValue());
      Assert.assertEquals(calendar.get(Calendar.DAY_OF_MONTH), localDate.getDayOfMonth());
    }
  }

  @Test
  public void testLocalDateToDate() {
    Calendar calendar = (Calendar) GMT_CALENDER.clone();
    for (LocalDate localDate : LOCAL_DATES) {
      java.sql.Date date =
          new java.sql.Date(OdpsTypeTransformer.localDateToDate(localDate, calendar).getTime());
      calendar.clear();
      calendar.setTime(date);

      Assert.assertEquals(localDate.getEra().getValue(), calendar.get(Calendar.ERA));
      boolean isBc = localDate.getEra() == IsoEra.BCE;
      Assert.assertEquals(isBc ? 1 : localDate.getYear(), calendar.get(Calendar.YEAR));
      Assert.assertEquals(localDate.getMonthValue(), calendar.get(Calendar.MONTH) + 1);
      Assert.assertEquals(localDate.getDayOfMonth(), calendar.get(Calendar.DAY_OF_MONTH));
    }
  }

  @Test
  public void testDateArrayCompatibility() {
    // ARRAY<DATE>
    TypeInfo typeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.DATE);
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", typeInfo));
    ArrayRecord r = new ArrayRecord(schema);

    List<java.sql.Date> dates = new ArrayList<>(DATES);
    dates.add(null);

    r.setArray(0, dates);
    compareDateLists(dates, r.getArray(0));
  }

  private void compareDateLists(List<java.sql.Date> list1, List<java.sql.Date> list2) {
    Assert.assertEquals(list1.size(), list2.size());
    for (int i = 0; i < list1.size(); i++) {
      Assert.assertEquals(list1.get(i), list2.get(i));
    }
  }

  @Test
  public void testDateMapCompatibility() {
    // MAP<DATE, BIGINT>
    TypeInfo dateToBigintTypeInfo =
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.DATE, TypeInfoFactory.BIGINT);
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", dateToBigintTypeInfo));
    ArrayRecord r = new ArrayRecord(schema);

    Map<java.sql.Date, Long> dateToBigint = getDateToRandomBigintMap();
    r.setMap(0, dateToBigint);
    compareDateToBigintMaps(dateToBigint, r.getMap(0));

    // MAP<BIGINT, DATE>
    TypeInfo bigintToDateTypeInfo =
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.DATE);
    schema = new TableSchema();
    schema.addColumn(new Column("col1", bigintToDateTypeInfo));
    r = new ArrayRecord(schema);

    Map<Long, java.sql.Date> bigintToDate = getRandomBigintToDateMap();
    r.setMap(0, bigintToDate);
    compareBigintToDateMaps(bigintToDate, r.getMap(0));
  }

  private Map<Long, java.sql.Date> getRandomBigintToDateMap() {
    Random random = new Random();
    Map<Long, java.sql.Date> bigintToDate = new HashMap<>(366 * 10000);
    bigintToDate.put(null, DATES.get(0));
    for (java.sql.Date date : DATES) {
      bigintToDate.put(random.nextLong(), date);
    }

    return bigintToDate;
  }

  private void compareBigintToDateMaps(
      Map<Long, java.sql.Date> map1, Map<Long, java.sql.Date> map2) {
    Assert.assertEquals(map1.size(), map2.size());
    for (Long bigint : map1.keySet()) {
      Assert.assertTrue(map2.containsKey(bigint));
      Assert.assertEquals(map1.get(bigint), map2.get(bigint));
    }
  }

  private Map<java.sql.Date, Long> getDateToRandomBigintMap() {
    Random random = new Random();
    Map<java.sql.Date, Long> dateToBigint = new HashMap<>(366 * 10000);
    dateToBigint.put(null, random.nextLong());
    for (java.sql.Date date : DATES) {
      dateToBigint.put(date, random.nextLong());
    }

    return dateToBigint;
  }

  private void compareDateToBigintMaps(
      Map<java.sql.Date, Long> map1, Map<java.sql.Date, Long> map2) {
    Assert.assertEquals(map1.size(), map2.size());
    for (java.sql.Date date : map1.keySet()) {
      Assert.assertTrue(map2.containsKey(date));
      Assert.assertEquals(map1.get(date), map2.get(date));
    }
  }

  @Test
  public void testDateStructCompatibility() {
    // ARRAY<DATE>
    TypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.DATE);

    // MAP<DATE, BIGINT>
    TypeInfo dateToBigintTypeInfo =
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.DATE, TypeInfoFactory.BIGINT);

    // MAP<BIGINT, DATE>
    TypeInfo bigintToDateTypeInfo =
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BIGINT, TypeInfoFactory.DATE);

    // STRUCT<ARRAY<DATE>, MAP<DATE, DATE>, DATE>
    TypeInfo structTypeInfo = TypeInfoFactory.getStructTypeInfo(
        Arrays.asList("array", "map1", "map2", "date", "null"),
        Arrays.asList(arrayTypeInfo, dateToBigintTypeInfo, bigintToDateTypeInfo, TypeInfoFactory.DATE, null));

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", structTypeInfo));
    ArrayRecord r = new ArrayRecord(schema);

    List<java.sql.Date> dates = new ArrayList<>(DATES);
    dates.add(null);
    Map<java.sql.Date, Long> dateToBigint = getDateToRandomBigintMap();
    Map<Long, java.sql.Date> bigintToDate = getRandomBigintToDateMap();
    Struct expected = new SimpleStruct(
        (StructTypeInfo) structTypeInfo,
        Arrays.asList(dates, dateToBigint, bigintToDate, DATES.get(0), null));

    r.setStruct(0, expected);
    Struct actual = r.getStruct(0);
    compareDateLists(
        (List<java.sql.Date>) expected.getFieldValue(0),
        (List<java.sql.Date>) actual.getFieldValue(0));
    compareDateToBigintMaps(
        (Map<java.sql.Date, Long>) expected.getFieldValue(1),
        (Map<java.sql.Date, Long>) actual.getFieldValue(1));
    compareBigintToDateMaps(
        (Map<Long, java.sql.Date>) expected.getFieldValue(2),
        (Map<Long, java.sql.Date>) actual.getFieldValue(2));
    Assert.assertEquals(expected.getFieldValue(3), actual.getFieldValue(3));
    Assert.assertNull(actual.getFieldValue(4));
  }

  @Test
  public void testNewDateTypeInArray() {
    TypeInfo arrayType = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.DATE);
    ArrayRecord r = new ArrayRecord(new Column[]{new Column("D", arrayType)});

    List<LocalDate> allLocalDate = new ArrayList<>();
    allLocalDate.add(LocalDate.now());
    r.set(0, allLocalDate);
    List dates1 = r.getArray(LocalDate.class, 0);
    Assert.assertEquals(LocalDate.class, dates1.get(0).getClass());

    List<java.sql.Date> allDate = new ArrayList<>();
    allDate.add(java.sql.Date.valueOf(LocalDate.now()));
    r.set(0, allDate);
    List dates2 = r.getArray(0);
    Assert.assertEquals(java.sql.Date.class, dates2.get(0).getClass());

    List mixDate = new ArrayList<>();
    mixDate.add(LocalDate.now());
    mixDate.add(java.sql.Date.valueOf(LocalDate.now()));
    r.set(0, mixDate);
    List dates3 = r.getArray(0);
    Assert.assertEquals(java.sql.Date.class, dates3.get(0).getClass());
    Assert.assertEquals(java.sql.Date.class, dates3.get(1).getClass());
    try {
      List dates4 = r.getArray(LocalDate.class, 0);
      assert false;
    } catch (ClassCastException ignore) {
    }
  }

  @Test
  public void testNewDateTypeInMap() {
    TypeInfo arrayType = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.TIMESTAMP, TypeInfoFactory.DATETIME);
    ArrayRecord r = new ArrayRecord(new Column[]{new Column("D", arrayType)});

    Map<Instant, ZonedDateTime> allNewType = new HashMap<>();
    allNewType.put(Instant.now(), Instant.now().atZone(ZoneId.systemDefault()));
    r.set(0, allNewType);
    Map dates1 = r.getMap(Instant.class, ZonedDateTime.class, 0);
    for (Object e: dates1.entrySet()) {
      Map.Entry entry = (Map.Entry) e;
      Assert.assertEquals(Instant.class, entry.getKey().getClass());
      Assert.assertEquals(ZonedDateTime.class, entry.getValue().getClass());
    }

    Map<Timestamp, Date> allOldType = new HashMap<>();
    allOldType.put(Timestamp.from(Instant.now()), Date.from(Instant.now()));
    r.set(0, allOldType);
    Map dates2 = r.getMap(0);
    for (Object e: dates2.entrySet()) {
      Map.Entry entry = (Map.Entry) e;
      Assert.assertEquals(Timestamp.class, entry.getKey().getClass());
      Assert.assertEquals(Date.class, entry.getValue().getClass());
    }

    Map mixType = new HashMap<>();
    mixType.put(Timestamp.from(Instant.now()), Date.from(Instant.now()));
    mixType.put(Instant.now(), Instant.now().atZone(ZoneId.systemDefault()));
    r.set(0, mixType);
    Map dates3 = r.getMap(0);
    for (Object e: dates3.entrySet()) {
      Map.Entry entry = (Map.Entry) e;
      Assert.assertEquals(Timestamp.class, entry.getKey().getClass());
      Assert.assertEquals(Date.class, entry.getValue().getClass());
    }
    try {
      Map dates4 = r.getMap(Instant.class, ZonedDateTime.class, 0);
      assert false;
    } catch (ClassCastException ignore) {
    }

    try {
      Map dates4 = r.getMap(Instant.class, Date.class, 0);
      assert false;
    } catch (IllegalArgumentException ignore) {
    }

    try {
      Map dates4 = r.getMap(Timestamp.class, ZonedDateTime.class, 0);
      assert false;
    } catch (IllegalArgumentException ignore) {
    }


  }

  @Test
  public void testVarcharOverFlow() {
    // make sure count char but not count length in varchar and char
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("varchar", TypeInfoFactory.getVarcharTypeInfo(11)));
    tableSchema.addColumn(new Column("char", TypeInfoFactory.getCharTypeInfo(11)));

    ArrayRecord r = new ArrayRecord(tableSchema);
    String str = "184了776991\uD873\uDC56";
    Assert.assertEquals(11, new Varchar(str).length());

    String str2 = "184了776991\uD83D\uDD3A";
    Assert.assertEquals(11, new Char(str2).length());

    r.set(0, new Varchar(str));
    r.set(1, new Char(str2));

    System.out.println(r);
  }
}
