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

package com.aliyun.odps.io;

import java.util.LinkedHashMap;

@Deprecated
public class TableInfo {

  public TableInfo(String tbl) {
  }

  public TableInfo(String tbl, String partSpec) {
  }

  public TableInfo(String tbl, LinkedHashMap<String, String> partSpec) {
  }

  public String getProjectName() {
    return null;
  }

  public String getTableName() {
    return null;
  }

  public void setTbl(String tbl) {
  }

  public String getPartSpec() {
    return null;
  }

  public void setPartSpec(String partSpec) {
  }

  public void setPartSpecMap(LinkedHashMap<String, String> partSpecMap) {
  }

  public LinkedHashMap<String, String> getPartSpecMap() {
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    return true;
  }

  @Override
  public String toString() {
    return null;
  }

}
