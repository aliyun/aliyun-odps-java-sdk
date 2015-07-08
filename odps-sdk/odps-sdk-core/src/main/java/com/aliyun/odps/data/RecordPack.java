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

import java.io.IOException;


/**
 * RecordPack 用于在内存中存放批量 Record
 */
public abstract class RecordPack {

  /**
   * 插入一条记录
   *
   * @param record
   * @throws IOException
   */
  abstract public void append(Record record) throws IOException;


  /**
   * 返回一个RecordReader，里面包含了所有的记录
   *
   * @return
   * @throws IOException
   */
  abstract public RecordReader getRecordReader() throws IOException;
}
