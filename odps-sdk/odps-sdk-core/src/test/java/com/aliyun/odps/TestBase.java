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

import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;

public class TestBase {

  protected static Odps odps;

  protected static final String ODPS_JAVA_SDK_UT_PREFIX = "ODPS_JAVA_SDK_UT_";

  @BeforeClass
  public static void setUpBeforeClassBase() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
  }

  public static Instance runQuery(String project, String sql) throws OdpsException {
    System.out.println("execute sql: " + sql);
    Instance i = SQLTask.run(odps, project, sql, null, null);
    System.out.println(new LogView(odps).generateLogView(i, 24 * 7));
    i.waitForSuccess();
    return i;
  }

  public static List<Record> runSelect(String project, String sql) throws OdpsException, IOException {
    Instance i = runQuery(project, sql);
    return SQLTask.getResultByInstanceTunnel(i);
  }

}
