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

package com.aliyun.odps.udf.local.datasource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.utils.PartitionUtils;
import com.aliyun.odps.udf.UDTFCollector;
import com.aliyun.odps.udf.local.LocalRunError;

public class UDTFTableCollector implements UDTFCollector {

  PrintWriter pw;
  Column[] schema;

  public UDTFTableCollector(String project, String table, String[] partitions) throws IOException {
    PartitionSpec partitionSpec = null;
    if (partitions != null && partitions.length > 0) {
      partitionSpec = new PartitionSpec();
      for (String p : partitions) {
        String[] kv = p.split("=");
        if (kv.length != 2) {
          throw new LocalRunError("Invalid partition:" + p + " of table " + project + "." + table);
        }
        partitionSpec.set(kv[0], kv[1]);
      }
    }

    WareHouse wareHouse = WareHouse.getInstance();
    if (partitions != null) {
      if (!wareHouse.existsPartition(project, table, partitionSpec)) {
        throw new LocalRunError(
            "partition " + project + "." + table + "(" + PartitionUtils.toString(partitionSpec)
            + ") not exists");
      }
      File partitionDir = wareHouse.getPartitionDir(project, table, partitionSpec);
      pw = new PrintWriter(new File(partitionDir, "data"));
    } else {
      if (!wareHouse.existsTable(project, table)) {
        throw new LocalRunError("output table:" + project + "." + table + " not exists");
      }
      File tableDir = wareHouse.getTableDir(project, table);
      pw = new PrintWriter(new File(tableDir, "data"));
    }

    schema = wareHouse.getTableMeta(project, table).getCols();

  }

  @Override
  public void collect(Object[] args) {

  }

}
