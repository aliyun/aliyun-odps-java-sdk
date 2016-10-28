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

package com.aliyun.odps.mapred.local;

import java.io.File;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mapred.cli.OdpsConf;
import com.aliyun.odps.mapred.conf.SessionState;

public class ExampleBase {

  static OdpsConf odpsConf;

  static {
    odpsConf = new OdpsConf();
    Account account = new AliyunAccount(odpsConf.getAccessId(),
                                        odpsConf.getAccessKey());
    Odps odps = new Odps(account);
    odps.setDefaultProject(odpsConf.getProjName());
    if (odpsConf.getEndpoint() != null) {
      odps.setEndpoint(odpsConf.getEndpoint());
    }
    SessionState.get().setOdps(odps);
    SessionState.get().setLocalRun(true);
  }

  public static String getInputTableName(Object object) {
    String result = object.getClass().getName();
    result = result.substring(result.lastIndexOf(".") + 1);
    return result.toLowerCase() + "_in";
  }

  public static String getOutputTableName(Object object) {
    String result = object.getClass().getName();
    result = result.substring(result.lastIndexOf(".") + 1);
    return result.toLowerCase() + "_out";
  }

  public static void init(Object object) {
    String root = System.getProperty("user.dir") + "/warehouse/"
                  + odpsConf.getProjName() + "/";
    File inputFile = new File(root + getInputTableName(object));
    if (!inputFile.exists()) {
      inputFile.mkdirs();
    }
    File outputFile = new File(root + getOutputTableName(object));
    if (!outputFile.exists()) {
      outputFile.mkdirs();
    }
    System.out.println("create input table and output table finished");
  }

}
