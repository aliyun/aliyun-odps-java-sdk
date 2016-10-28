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

package com.aliyun.odps.mapred.unittest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

/**
 * {@link Mapper} 单元测试的上下文.
 * 
 * <p>
 * 编写 {@link Mapper} 单元测试需要用到此上下文。
 * 
 * @see MRUnitTest#runMapper(com.aliyun.odps.mapreduce.JobConf, MapUTContext)
 * 
 */
public class MapUTContext extends UTContext {

  private List<Record> records = new ArrayList<Record>();
  
  private String inputSchema = null;

  /**
   * 增加 Map 输入记录.
   * 
   * @param record
   */
  public void addInputRecord(Record record) {
    records.add(record);
  }

  /**
   * 增加 Map 输入记录.
   * 
   * @param records
   */
  public void addInputRecords(Collection<Record> records) {
    this.records.addAll(records);
  }

  /**
   * 给定一个本地表目录，增加 Map 输入记录.
   * 
   * @param dir
   * @throws IOException
   */
  public void addInputRecordsFromDir(File dir) throws IOException {
    if (!dir.exists()) {
      throw new RuntimeException(dir.getAbsolutePath() + " : input dir not exists!");
    }
    addInputRecords(MRUnitTest.readRecords(dir));
  }

  /**
   * 获取 Map 输入的记录列表.
   * 
   * @return
   */
  public List<Record> getInputRecords() {
    return records;
  }

  /**
   * 清空 Map 输入记录.
   */
  public void clearInputRecords() {
    records.clear();
  }

  /**
   * 设置 Map 输入的 schema.
   * 
   * <p>
   * schema 的格式为：<col_name>:<col_type >(,<col_name>:<col_type>)*<br />
   * 例如：a:string,b:bigint,c:double
   * 
   * @param inputSchema
   *          Map 输入的 schema
   * @throws IOException
   */
  public void setInputSchema(String inputSchema) throws IOException {
    try {
      com.aliyun.odps.mapred.utils.SchemaUtils.fromString(inputSchema.trim());
    } catch (Exception ex) {
      throw new IOException("bad schema format: " + inputSchema);
    }
    this.inputSchema = inputSchema;
  }

  public String getInputSchema() {
    return inputSchema;
  }

  /**
   * 创建 Map 输入的记录对象.
   * 
   * <p>
   * 记录的 schema 通过 {@link #setInputSchema(String)} 设置。
   * 
   * @return Map 输入的记录对象
   * @throws IOException
   */
  public Record createInputRecord() throws IOException {
    if (inputSchema == null) {
      throw new IOException("input schema is not set.");
    }
    return MRUnitTest.createRecord(inputSchema);
  }
}
