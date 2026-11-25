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

import java.io.IOException;
import java.io.InputStream;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.arrow.compression.OdpsCompressionFactory;
import com.aliyun.odps.table.arrow.readers.ExtendedArrowReader;
import com.aliyun.odps.table.read.ArrowStreamRecordReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/** Storage session adapter for negotiated extended Arrow IPC streams. */
final class ExtendedTableArrowReader implements ArrowReader {
  private final ExtendedArrowReader reader;
  private final TableSchema tableSchema;

  ExtendedTableArrowReader(TableReaderBuilder builder, InputStream input) {
    reader = new ExtendedArrowReader(input, builder.getAllocator(),
        OdpsCompressionFactory.V2_READ_INSTANCE, builder.isAsync(), builder.getAsyncQueue());
    tableSchema = builder.getTableSchema();
  }

  @Override
  public VectorSchemaRoot getCurrentValue() {
    return reader.getCurrentValue();
  }

  @Override
  public boolean nextBatch() throws IOException {
    return reader.nextBatch();
  }

  @Override
  public long bytesRead() {
    return reader.bytesRead();
  }

  @Override
  public Schema getSchema() {
    return reader.getSchema();
  }

  @Override
  public RecordReader getAsRecordReader() {
    return new ArrowStreamRecordReader(this, tableSchema, null, true, false);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public void close(boolean closeReadSource) throws IOException {
    reader.close(closeReadSource);
  }
}
