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

package com.aliyun.odps.mapred.local;

import com.aliyun.odps.local.common.utils.TypeConvertUtils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.local.common.FileSplit;
import com.aliyun.odps.local.common.TableMeta;
import com.aliyun.odps.local.common.utils.DownloadUtils;
import com.aliyun.odps.mapred.bridge.WritableRecord;
import com.csvreader.CsvReader;

public class CSVRecordReader implements RecordReader {

  private Object[] value;
  private Counter recordCounter;
  private CsvReader csvReader;
  private FileSplit split;
  private TableMeta tableMeta;
  private Record record;
  private Counters counters;

  public static final Charset encoding = Charset.forName("UTF-8");

  public CSVRecordReader(FileSplit split, TableMeta tableMeta, Counter recordCounter,
                         Counter byteCounter, Counters counters, char inputColumnSeperator)
      throws IOException {
    this.split = split;
    this.tableMeta = tableMeta;
    this.record = new WritableRecord(split.getSchema());
    File file = split.getFile();
    if (file != null) {
      if (byteCounter != null) {
        byteCounter.increment(file.length());
      }
      csvReader = DownloadUtils.newCsvReader(file.getAbsolutePath(), inputColumnSeperator, encoding);
      this.recordCounter = recordCounter;
    }
    this.counters = counters;
  }

  @Override
  public Record read() throws IOException {
    if (csvReader == null) {
      return null;
    }

    boolean hasNext = csvReader.readRecord();
    if (!hasNext) {
      return null;
    }
    if (recordCounter != null) {
      recordCounter.increment(1);
    }
    ;
    String[] vals = csvReader.getValues();
    int size = split.getSchema().length;
    if (vals.length != size) {
      if (tableMeta != null) {
        throw new IOException("column num of table '" + tableMeta.getProjName() + "."
            + tableMeta.getTableName() + "' not match, expect: " + size + ", but: " + vals.length);
      } else {
        throw new IOException("column num not match, expect: " + size + ", but: " + vals.length);
      }
    }
    value = new Object[size];
    for (int i = 0; i < size; i++) {
      value[i] = TypeConvertUtils.fromString(split.getSchema()[i].getTypeInfo(),
                                          vals[i], true);
    }
    record.set(value);

    counters.findCounter(JobCounter.__EMPTY_INPUT_RECORD_COUNT).increment(1);

    return record;
  }

  @Override
  public void close() throws IOException {
    if (csvReader != null) {
      csvReader.close();
    }
  }
}
