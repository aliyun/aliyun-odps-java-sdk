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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.sqa.commandapi.CommandInfo;
import com.aliyun.odps.type.TypeInfo;


public interface Command {

  boolean isSync();

  /**
   * 结果集的列名。和列类型一一对应。
   *
   * @return
   */
  List<String> getResultHeaders();

  /**
   * 结果集的列类型。和列名一一对应。
   *
   * @return
   */
  List<TypeInfo> getResultTypes();

  RecordIter run(Odps odps, CommandInfo commandInfo) throws OdpsException;

}
