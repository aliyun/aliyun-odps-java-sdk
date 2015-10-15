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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Column;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.graph.DataType;
import com.aliyun.odps.graph.local.utils.LocalGraphRunUtils;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.local.common.utils.DownloadUtils;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.aliyun.odps.utils.ReflectionUtils;
import com.csvreader.CsvReader;

public class LocalRecordReader extends BaseRecordReader<LongWritable, WritableRecord> {

  private LongWritable key;
  private SQLRecord value;
  private Counter recordCounter;
  private List<CsvReader> readers;
  private CsvReader reader;
  private int nextReaderIdx = 0;
  private byte[] types;

  private long count = 0;
  private boolean closed = false;

  public LocalRecordReader(File inputDir, File tableDir, Counter recordCounter,
                           Counter byteCounter) throws IOException {
    this.key = new LongWritable();
    Column[] cols = SchemaUtils.readSchema(tableDir).getCols();
    this.types = new byte[cols.length];
    Map<String, Integer> mapIdx = new HashMap<String, Integer>();
    for (int i = 0; i < cols.length; i++) {
      mapIdx.put(cols[i].getName(), i);
      types[i] = DataType.convertToDataType(cols[i].getType().name());
    }
    this.value = new SQLRecord(cols, types);
    readers = new ArrayList<CsvReader>();
    for (File dataFile : LocalRunUtils.listDataFiles(inputDir)) {
      if (byteCounter != null) {
        byteCounter.increment(dataFile.length());
      }
      Charset encoding = LocalRunContext.getInstance().getEncoding();
      CsvReader csvReader = DownloadUtils.newCsvReader(dataFile.getAbsolutePath(), ',', encoding);
      readers.add(csvReader);
    }
    if (readers.size() > 0) {
      reader = readers.get(0);
      nextReaderIdx++;
    }
    this.recordCounter = recordCounter;
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (readers.size() == 0) {
      return false;
    }

    boolean hasNext = reader.readRecord();
    if (!hasNext && nextReaderIdx < readers.size()) {
      reader = readers.get(nextReaderIdx++);
      hasNext = reader.readRecord();
    }

    if (hasNext) {
      count++;
      key.set(count);
      String[] vals = reader.getValues();
      if (vals.length != types.length) {
        throw new IOException("column num not match, expect: " + types.length
                              + ", but: " + vals.length);
      }
      Writable[] fields = new Writable[types.length];
      for (int i = 0; i < types.length; i++) {
        fields[i] = LocalGraphRunUtils.fromString(types[i], vals[i], "\\N");
      }

      for (int i = 0; i < fields.length; i++) {
        Writable field = value.get(i);
        if (field == null) {
          value.set(i, fields[i]);
        } else {
          if (fields[i] == null) {
            value.set(i, null);
          } else {
            ReflectionUtils.cloneWritableInto(field, fields[i]);
          }
        }
      }
    }
    return hasNext;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException {
    return key;
  }

  @Override
  public WritableRecord getCurrentValue() throws IOException {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    return 0.0f;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      if (recordCounter != null) {
        recordCounter.setValue(count);
      }
      for (CsvReader reader : readers) {
        reader.close();
      }
    }
  }

}
