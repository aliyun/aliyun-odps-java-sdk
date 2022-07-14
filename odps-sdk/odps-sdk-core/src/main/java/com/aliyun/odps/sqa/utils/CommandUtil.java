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

package com.aliyun.odps.sqa.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

public class CommandUtil {

  private static final Calendar calendar = new Calendar.Builder().setCalendarType("iso8601")
      .setLenient(true).setTimeZone(TimeZone.getTimeZone("GMT")).build();

  public static String runJob(Task task, Odps odps) throws OdpsException {
    Instance instance = submit(task, odps);
    return instance.getId();
  }

  private static Instance submit(Task task, Odps odps) throws OdpsException {
    Job job = new Job();
    String guid = UUID.randomUUID().toString();
    task.setProperty("guid", guid);
    job.addTask(task);

    return odps.instances().create(job);
  }

  /**
   * @param data    数据
   * @param headers 数据对应的表头
   * @return
   */
  public static List<Record> toRecord(List<List<String>> data, List<String> headers) {
    if (headers == null || headers.size() == 0) {
      throw new IllegalArgumentException("headers is empty!");
    }

    List<Record> records = new ArrayList<>();

    Column[] columns = new Column[headers.size()];
    int index = 0;
    for (String header : headers) {
      columns[index++] = new Column(header, TypeInfoFactory.STRING);
    }

    for (List<String> subData : data) {
      if (subData == null || subData.size() != headers.size()) {
        throw new IllegalArgumentException("data and headers not match");
      }
      Record record = new ArrayRecord(columns);
      for (int i = 0; i < subData.size(); i++) {
        record.set(i, subData.get(i));
      }
      records.add(record);
    }

    return records;
  }

  public static List<Record> toRecord(String data) {
    Column[] columns = new Column[1];
    columns[0] = new Column("Info", TypeInfoFactory.STRING);
    Record record = new ArrayRecord(columns);
    record.set(0, data);

    return Collections.singletonList(record);
  }

  /**
   * @param data     数据
   * @param dataType 数据对应的类型, 有序
   * @return
   */
  public static List<Record> toRecord(Map<String, Object> data, Map<String, TypeInfo> dataType) {
    if (dataType == null || dataType.size() == 0) {
      throw new IllegalArgumentException("dataType is empty!");
    }

    Column[] columns = new Column[dataType.keySet().size()];
    int index = 0;
    for (String header : dataType.keySet()) {
      columns[index++] = new Column(header, dataType.get(header));
    }

    Record record = new ArrayRecord(columns);
    index = 0;
    for (String header : dataType.keySet()) {
      if (!data.containsKey(header)) {
        throw new IllegalArgumentException("data and dataType not match.");
      }
      record.set(index++, data.get(header));
    }

    return Collections.singletonList(record);
  }

  public static List<Record> toRecord(List<Map<String, Object>> data,
      Map<String, TypeInfo> dataType) {

    if (dataType == null || dataType.size() == 0) {
      throw new IllegalArgumentException("dataType is empty!");
    }

    Column[] columns = new Column[dataType.keySet().size()];
    int index = 0;
    for (String header : dataType.keySet()) {
      columns[index++] = new Column(header, dataType.get(header));
    }

    List<Record> records = new ArrayList<>();
    for (Map<String, Object> dataMap : data) {
      if (dataMap == null || dataMap.size() != dataType.size()) {
        throw new IllegalArgumentException("data and dataType not match");
      }
      Record record = new ArrayRecord(columns);
      index = 0;
      for (String header : dataType.keySet()) {
        record.set(index++, dataMap.get(header));
      }
      records.add(record);
    }

    return records;
  }

  public static Date getTime(Date date, int hour) {
    if (date == null) {
      return null;
    }
    Calendar cal = (Calendar) calendar.clone();
    cal.setTime(date);
    cal.set(Calendar.HOUR_OF_DAY, hour);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  public static Date toDate(String strDate) {
    if (strDate == null) {
      return null;
    }
    try {
      Date date = new SimpleDateFormat("yyyy-MM-dd").parse(strDate);
      Calendar cal = (Calendar) calendar.clone();
      cal.setTime(date);
      return cal.getTime();
    } catch (ParseException e) {
      throw new RuntimeException("invalid date: " + strDate);
    }
  }

  public static int getNumberToken(String numberString) {
    int number;
    try {
      number = Integer.parseInt(numberString);
      if (number < 1) {
        throw new IllegalArgumentException("number should >= 1");
      }
    } catch (NumberFormatException e) {
      throw new RuntimeException("number is not integer");
    }
    return number;
  }

}
