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

package com.aliyun.odps.sqa.commandapi.utils;

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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.sqa.commandapi.antlr.CommandLexer;
import com.aliyun.odps.sqa.commandapi.antlr.CommandParser;
import com.aliyun.odps.sqa.commandapi.exception.CommandErrorStrategy;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.sqa.commandapi.Command;
import com.aliyun.odps.sqa.commandapi.CommandParserListener;
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
   * 解析command
   * 在开启三层模型开关后，获得的并不一定是最后符合要求的command
   *
   * @param commandText command
   * @return 解析失败则返回null
   */
  public static Command parseCommand(String commandText) {
    Command command = null;

    try {
      CommandParserListener commandParserListener = getCommandParserListener(commandText);
      // 增加三层模型开关后，解析语句获得的并非真正的Command，因为对应的project、schema不一定是对
      command = commandParserListener.getCommand();
    } catch (RuntimeException ignored) {
      // 抛异常则说明解析失败，返回null
    }
    return command;
  }

  /**
   * 开启三层模型后对schema进行赋值
   *
   * @param odps
   * @param project 语法解析侧的project名称，并不一定对应真正的project
   * @param schema  语法解析侧的schema名称，并不一定对应真正的schema
   * @return 返回真正的schema
   */
  private static String getRealSchemaName(Odps odps, String project, String schema) {
    if (null == schema) {
      if (null == project) {
        return odps.getCurrentSchema();
      } else {
        return project;
      }
    } else {
      return schema;
    }
  }

  /**
   * 获取真正的schema name
   *
   * @param odps
   * @param project
   * @param schema
   * @param useOdpsNamespaceSchema
   * @return
   */
  public static String getRealSchemaName(Odps odps, String project, String schema,
                                         boolean useOdpsNamespaceSchema) {
    if (useOdpsNamespaceSchema) {
      return getRealSchemaName(odps, project, schema);
    }
    return schema;
  }

  /**
   * 获取真正的project name
   *
   * @param odps
   * @param project
   * @param schema
   * @param useOdpsNamespaceSchema
   * @return
   */
  public static String getRealProjectName(Odps odps, String project, String schema,
                                          boolean useOdpsNamespaceSchema) {
    if (useOdpsNamespaceSchema) {
      return schema == null ? odps.getDefaultProject() : project;
    }
    return project == null ? odps.getDefaultProject() : project;

  }

  /**
   * 采用的语法树是Command语法树
   *
   * @param commandText command
   * @return
   */
  private static CommandParserListener getCommandParserListener(String commandText) {
    ANTLRInputStream input = new ANTLRInputStream(commandText);
    CommandLexer lexer = new CommandLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    CommandParser
        parser =
        new CommandParser(tokens);
    parser.setErrorHandler(new CommandErrorStrategy());

    CommandParserListener checkCommandListener = new CommandParserListener();
    ParseTreeWalker treeWalker = new ParseTreeWalker();
    treeWalker.walk(checkCommandListener, parser.command());
    return checkCommandListener;
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

  /**
   * 目前仅用于异步Command构造Record
   *
   * @param data 数据
   * @param name 列名，默认列类型为String, 列数目为1
   * @return
   */
  public static List<Record> toRecord(String data, String name) {
    Column[] columns = new Column[1];
    columns[0] = new Column(name, TypeInfoFactory.STRING);
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
