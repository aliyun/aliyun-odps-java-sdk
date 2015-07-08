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

package com.aliyun.odps.graph.local;

import java.io.File;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.FileSplit;

public class InputSplit extends FileSplit {

  public static final InputSplit NullSplit = new InputSplit(null,
                                                            new Column[]{
                                                                new Column("nil", OdpsType.STRING)},
                                                            0, 0, null);

  private TableInfo table;
  private String partSpec;

  public String getPartSpec() {
    return partSpec;
  }

  public void setPartSpec(String partSpec) {
    this.partSpec = partSpec;
  }

  public TableInfo getTable() {
    return table;
  }

  public void setTable(TableInfo table) {
    this.table = table;
  }

  InputSplit(File file, Column[] schema, long start, long length, TableInfo table) {
    super(file, schema, start, length);
    this.table = table;
  }

}
