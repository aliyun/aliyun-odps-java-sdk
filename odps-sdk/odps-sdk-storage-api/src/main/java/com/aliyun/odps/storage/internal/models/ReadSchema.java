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

package com.aliyun.odps.storage.internal.models;

import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ReadSchema extends TableSchema {

  private List<Column> blobColumns;

  private List<Column> systemColumns;

  public List<Column> getSystemColumns() {
    return systemColumns;
  }

  public void setSystemColumns(List<Column> systemColumns) {
    this.systemColumns = systemColumns;
  }

  public List<Column> getBlobColumns() {
    return blobColumns;
  }

  public void setBlobColumns(List<Column> blobColumns) {
    this.blobColumns = blobColumns;
  }

  @Override
  public List<Column> getAllColumns() {
    List<Column> allColumns = super.getAllColumns();
    allColumns.addAll(systemColumns);
    return allColumns;
  }
}
