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

package com.aliyun.odps.sqa.commandapi;

import java.io.IOException;
import java.util.List;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * 测试使用：<br/> 1. 打印record<br/> 2. 计算不通过语法解析的command数目<br/>
 */
public class CommandTestUtil {

  private static SQLExecutor getSQLExecutor(Odps odps) throws OdpsException {
    return SQLExecutorBuilder.builder().enableCommandApi(true).executeMode(
        ExecuteMode.INTERACTIVE).odps(odps).build();
  }

  public static void printRecord(Record record) {
    for (int k = 0; k < record.getColumnCount(); k++) {

      if (k != 0) {
        System.out.print("\t");
      }

      if (record.getColumns()[k].getTypeInfo().equals(TypeInfoFactory.STRING)) {
        System.out.print(record.getString(k));
      } else if (record.getColumns()[k].getTypeInfo().equals(TypeInfoFactory.BIGINT)) {
        System.out.print(record.getBigint(k));
      } else if (record.getColumns()[k].getTypeInfo().equals(TypeInfoFactory.DATE)) {
        System.out.print(((ArrayRecord) record).getDate(k));
      } else if (record.getColumns()[k].getTypeInfo().equals(TypeInfoFactory.BOOLEAN)) {
        System.out.print(record.getBoolean(k));
      } else if (record.getColumns()[k].getTypeInfo().equals(TypeInfoFactory.DATETIME)) {
        System.out.print(record.getDatetime(k));
      } else {
        System.out.print(record.get(k));
      }
    }
  }

  public static void printRecords(List<Record> records) {
    if (records == null) {
      return;
    }
    for (Record record : records) {
      System.out.println();
      printRecord(record);
    }
  }

  public static int getErrorNum(String[] commands) {
    int errorNum = 0;
    for (String command : commands) {
      if (CommandUtil.parseCommand(command) == null) {
        errorNum++;
      }
    }
    return errorNum;
  }

  public static List<Record> runCommandAndGetResult(Odps odps, String command) {
    try {
      SQLExecutor sqlExecutor = getSQLExecutor(odps);
      sqlExecutor.run(command, null);
      return sqlExecutor.getResult();
    } catch (OdpsException e) {
      e.printStackTrace();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public static void runCommandAndPrintResult(Odps odps, String command) {
    CommandTestUtil.printRecords(runCommandAndGetResult(odps, command));
  }

  public static ResultSet runCommandAndGetResultSet(Odps odps, String command) {
    try {
      SQLExecutor sqlExecutor = getSQLExecutor(odps);
      sqlExecutor.run(command, null);
      return sqlExecutor.getResultSet();
    } catch (OdpsException e) {
      e.printStackTrace();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public static void runCommandAndPrintResultSet(Odps odps, String command) {
    ResultSet resultSet = runCommandAndGetResultSet(odps, command);
    if (resultSet == null) {
      throw new RuntimeException("resultSet is null");
    }
    while (resultSet.hasNext()) {
      Record record = resultSet.next();
      CommandTestUtil.printRecord(record);
      System.out.println();
    }
  }

  public static void runErrorCommand(Odps odps, String command)
      throws OdpsException, IOException {
    SQLExecutor sqlExecutor = getSQLExecutor(odps);
    sqlExecutor.run(command, null);
    List<Record> records = sqlExecutor.getResult();
    CommandTestUtil.printRecords(records);
  }
}
