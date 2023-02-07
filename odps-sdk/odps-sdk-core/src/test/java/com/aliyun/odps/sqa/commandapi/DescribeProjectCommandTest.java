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

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;

/**
 * 测试包括三个方面： 1. 语法解析是否可以通过 2. 通过语法解析的正确的command能否返回正确的结果 3. 通过语法解析但是错误的command能否及时抛异常
 */
public class DescribeProjectCommandTest extends TestBase {

  private static final String TEST_PROJECT_NAME = odps.getDefaultProject();
  private static final String TEST_PROJECT_NAME_NOT_EXIST = odps.getDefaultProject() + "_not_exist_in_descCommandTest";

  @Test
  public void describeProjectCommandTest() throws OdpsException {
    String[] positiveCommands = new String[]{
        "desc project extended " + TEST_PROJECT_NAME + " ;",
        "DESC project " + TEST_PROJECT_NAME + " ;",
        "describe PROJECT extended " + TEST_PROJECT_NAME + " ;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = new String[]{
        "desc project -extended " + TEST_PROJECT_NAME + " ;",
        "DESC projects " + TEST_PROJECT_NAME + " ;",
        "describes PROJECT extended " + TEST_PROJECT_NAME + " ;"
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));

    int numOfNull = 0;
    for (String positiveCommand : positiveCommands) {
      List<Record> recordList = CommandTestUtil.runCommandAndGetResult(odps, positiveCommand);
      if (recordList == null || recordList.size() == 0) {
        numOfNull++;
        continue;
      }
      for (Record record : recordList) {
        Assert.assertEquals(record.get(0), odps.projects().get().getName());
        Assert.assertEquals(record.get(3), odps.projects().get().getOwner());
        Assert.assertEquals(record.get(4), odps.projects().get().getCreatedTime());
      }
    }
    Assert.assertEquals(numOfNull, 0);

    numOfNull = 0;
    for (String positiveCommand : positiveCommands) {
      ResultSet resultSet = CommandTestUtil.runCommandAndGetResultSet(odps, positiveCommand);
      if (resultSet == null) {
        numOfNull++;
        continue;
      }
      while (resultSet.hasNext()) {
        Record record = resultSet.next();
        Assert.assertEquals(record.get(0), odps.projects().get().getName());
        Assert.assertEquals(record.get(3), odps.projects().get().getOwner());
        Assert.assertEquals(record.get(4), odps.projects().get().getCreatedTime());
      }
    }
    Assert.assertEquals(numOfNull, 0);

    String[] errorCommands = new String[]{
        "desc project " + TEST_PROJECT_NAME_NOT_EXIST + " ;",
        "desc project extended" + TEST_PROJECT_NAME_NOT_EXIST + " ;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(errorCommands));
    int num = 0;
    for (String errorCommand : errorCommands) {
      try {
        CommandTestUtil.runErrorCommand(odps, errorCommand);
      } catch (Exception e) {
        num++;
      }
    }
    Assert.assertEquals(errorCommands.length, num);

  }
}