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

package com.aliyun.odps.storage.read;

import java.io.InputStream;

import org.apache.arrow.memory.BufferAllocator;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.table.arrow.compression.OdpsCompressionFactory;
import com.aliyun.odps.table.arrow.readers.ArrowBatchReusedReader;
import com.aliyun.odps.table.read.ArrowStreamRecordReader;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ArrowReaderImpl extends ArrowBatchReusedReader {

  private TableSchema tableSchema;

  public ArrowReaderImpl(TableReaderBuilder builder, InputStream is) {
    super(is, builder.getAllocator(), OdpsCompressionFactory.INSTANCE, builder.isAsync(),
          builder.getAsyncQueue());
    this.tableSchema = builder.getTableSchema();
  }

  public ArrowReaderImpl(InstanceReaderBuilder builder, InputStream is) {
    super(is, builder.getAllocator(), OdpsCompressionFactory.INSTANCE, builder.isAsync(),
          builder.getAsyncQueue());
    this.tableSchema = builder.getTableSchema();
  }

  public ArrowReaderImpl(InputStream is, BufferAllocator allocator) {
    super(is, allocator, OdpsCompressionFactory.INSTANCE, false,
          null);
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public RecordReader getAsRecordReader() {
    if (tableSchema == null) {
      throw new ClientException(
        "`getAsRecordReader` need provide `TableSchema`，you can use `new ArrowStreamRecordReader(ArrowReader, TableSchema, null, true, false)` instead.");
    }
    return new ArrowStreamRecordReader(this, tableSchema, null, true, false);
  }
}
