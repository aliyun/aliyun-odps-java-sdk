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

package com.aliyun.odps.data;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.PartitionSpec;

public class TableInfoTest {

  @Test
  public void testGetPartitionSpec() {
    PartitionSpec partitionSpec = new PartitionSpec("p2='1',p1='2'");
    TableInfo tableInfo = TableInfo.builder().projectName("project1").tableName("table1")
        .partSpec(partitionSpec).build();
    Assert.assertEquals("project1", tableInfo.getProjectName());
    Assert.assertEquals("table1", tableInfo.getTableName());
    partitionSpec = tableInfo.getPartitionSpec();
    int i = 0;
    for (String key : partitionSpec.keys()) {
      if (i == 0) {
        Assert.assertEquals("p2=1", key + "=" + partitionSpec.get(key));
      } else {
        Assert.assertEquals("p1=2", key + "=" + partitionSpec.get(key));
      }
      i++;
    }

    partitionSpec = null;
    tableInfo = TableInfo.builder().projectName("project2").tableName("table2")
        .partSpec(partitionSpec).build();
    Assert.assertEquals("project2", tableInfo.getProjectName());
    Assert.assertEquals("table2", tableInfo.getTableName());
    partitionSpec = tableInfo.getPartitionSpec();
    Assert.assertEquals(true, partitionSpec.isEmpty());

  }
  
  @Test
  public void testTableName() {
    TableInfo info = new TableInfo();
    
    // normal tablename
    info.setTableName("table");
    Assert.assertTrue(info.getProjectName() == null);
    Assert.assertEquals("table", info.getTableName());
    
    // projectname.tablename
    info.setTableName("prj.table");
    Assert.assertEquals("prj", info.getProjectName());
    Assert.assertEquals("table", info.getTableName());
  }

}
