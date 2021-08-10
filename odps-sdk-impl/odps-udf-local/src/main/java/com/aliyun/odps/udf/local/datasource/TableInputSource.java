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

import java.io.IOException;
import java.util.Date;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.local.common.utils.PartitionUtils;
import com.aliyun.odps.udf.local.LocalRunError;
import com.aliyun.odps.udf.local.LocalRunException;

public class TableInputSource extends InputSource {

  private List<Object[]> dataList;
  private int totalCount;
  private int index = 0;

  public TableInputSource(TableInfo tableInfo) {
    if (tableInfo == null) {
      throw new LocalRunError("Invalid input table info");
    }

    if (StringUtils.isBlank(tableInfo.getProjectName())) {
      throw new LocalRunError("Project can't be null");
    }

    if (StringUtils.isBlank(tableInfo.getTableName())) {
      throw new LocalRunError("Input table can't be null");
    }

    try {
      dataList =
          WareHouse.getInstance().readData(tableInfo.getProjectName(), tableInfo.getTableName(),
              tableInfo.getPartitionSpec(), tableInfo.getCols(), WareHouse.getInstance().getInputColumnSeperator());
    } catch (OdpsException e) {
      throw new LocalRunError(e.getMessage());
    } catch (IOException e) {
      throw new LocalRunError(e.getMessage());
    }
    if (dataList == null || dataList.size() == 0) {
      totalCount = 0;
    } else {
      totalCount = dataList.size();
    }

  }

  /**
   * @param project
   * @param table
   * @param partitions
   * @param columns
   * @throws LocalRunException
   */
  public TableInputSource(String project, String table, String[] partitions, String[] columns)
      throws LocalRunException {
    this(TableInfo.builder().projectName(project).tableName(table)
        .partSpec(PartitionUtils.convert(partitions)).cols(columns).build());

  }

  @Override
  public Object[] getNextRow() throws IOException {

    if (totalCount == 0 || index >= totalCount) {
      return null;
    }
    return dataList.get(index++);

  }

}
