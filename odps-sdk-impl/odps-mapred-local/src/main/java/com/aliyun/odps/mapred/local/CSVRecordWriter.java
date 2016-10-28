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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordPack;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.local.common.Constants;
import com.aliyun.odps.local.common.utils.LocalRunUtils;
import com.csvreader.CsvWriter;

public class CSVRecordWriter implements RecordWriter {

  private CsvWriter writer;
  private boolean closed = false;
  private File outputFile;
  private Counter recordCounter;
  private Counter byteCounter;
  private long count = 0;

  public CSVRecordWriter(File file, Counter recordCounter, Counter byteCounter,
                         char outputColumnSeperator)
      throws IOException {
    this.recordCounter = recordCounter;
    this.byteCounter = byteCounter;
    outputFile = new File(file.getAbsolutePath());
    Charset encoding = Charset.forName("UTF-8");
    writer = new CsvWriter(file.getAbsolutePath(), outputColumnSeperator, encoding);
  }

  @Override
  public void write(Record record) throws IOException {
    Object[] fields = record.toArray();
    String[] vals = new String[fields.length];
    for (int i = 0; i < fields.length; i++) {
      String rawVal;
      if (record.getColumns()[i].getType() == OdpsType.DATETIME) {
        rawVal = fields[i] == null ? null : LocalRunUtils.getDateFormat(Constants.DATE_FORMAT_2)
            .format(fields[i]);
      } else if (record.getColumns()[i].getType() == OdpsType.STRING) {
        try {
          rawVal = fields[i] == null ? null : LocalRunUtils.toReadableString(record.getBytes(i));
        } catch (Exception e) {
          throw new RuntimeException("convert to readable string failed!" + e);
        }
      } else {
        rawVal = fields[i] == null ? null : fields[i].toString();
      }
      vals[i] = encodeColumnValue(rawVal);
    }
    writer.writeRecord(vals);
    ++count;
  }

  public void write(RecordPack pack) throws IOException {
    Record record;
    RecordReader reader = pack.getRecordReader();
    while ((record = reader.read()) != null) {
      write(record);
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      writer.close();
      if (recordCounter != null) {
        recordCounter.increment(count);
      }
      if (byteCounter != null) {
        byteCounter.increment(getLen());
      }
    }
  }

  public long getLen() throws IOException {
    return outputFile.length();
  }

  /**
   * Encode:replace \N with "\N", exception column is null.
   */
  private static String encodeColumnValue(String val) {
    if (val == null) {
      return Constants.NULL_TOKEN;
    } else {
      return val.replaceAll("\\\\N", "\"\\\\N\"");
    }
  }
}
