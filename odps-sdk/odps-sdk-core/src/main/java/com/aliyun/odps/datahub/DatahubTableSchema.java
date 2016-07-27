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

package com.aliyun.odps.datahub;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;

class DatahubTableSchema extends TableSchema {

  public DatahubTableSchema(JSONObject node) {
    Object columns = node.get("columns");
    if (columns instanceof JSONArray) {
      JSONArray list = (JSONArray) columns;
      for (int i = 0; i < list.size(); ++i) {

        JSONObject column = list.getJSONObject(i);
        String name = column.getString("name");
        String type = column.getString("type");

        addColumn(new Column(name, OdpsType.valueOf(type.toUpperCase())));
      }
    }
    Object partitions = node.get("partitionKeys");
    if (partitions instanceof JSONArray) {
      JSONArray list = (JSONArray) partitions;
      for (int i = 0; i < list.size(); ++i) {

        JSONObject column = list.getJSONObject(i);
        String name = column.getString("name");
        String type = column.getString("type");

        addPartitionColumn(new Column(name, OdpsType.valueOf(type.toUpperCase())));
      }
    }
  }
}
