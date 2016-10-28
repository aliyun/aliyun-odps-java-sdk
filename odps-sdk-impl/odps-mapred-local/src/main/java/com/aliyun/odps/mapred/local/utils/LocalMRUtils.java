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

package com.aliyun.odps.mapred.local.utils;

import java.util.Date;

import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.utils.LocalRunUtils;

public class LocalMRUtils {

  public static String generateMrTaskName() {
    return "console_mr_" + LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_1).format(new Date());
  }

  public static void generateLocalMrTaskName() {
    WareHouse.getInstance().setJobName(
        "mr_" + LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_1).format(new Date()) + "_"
        + LocalRunUtils.getPID());
  }
}
