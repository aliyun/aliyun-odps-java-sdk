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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.csvreader.CsvReader;

/**
 * 默认 {@link RecordReader} 的实现，用于读取 Record
 */
public class DefaultRecordReader implements RecordReader {

  private static char QUOTE = '"';
  private static String NULL_TOKEN = "\\N";
  private CsvReader reader;
  private static final String CHARSET = "utf-8";
  private TableSchema tableSchema;
  private Column[] schemaColumns = null;
  private InputStream is;

  /**
   * 构造一个 DefaultRecordReader
   *
   * @param is
   *     输入流
   * @param tableSchema
   *     {@link TableSchema}
   */
  public DefaultRecordReader(InputStream is, TableSchema tableSchema) {

    try {
      this.is = is;
      this.reader = new CsvReader(new InputStreamReader(is, CHARSET), ',');
      this.reader.setSafetySwitch(false);
    } catch (UnsupportedEncodingException e) {
      // do nothing
    }

    this.tableSchema = tableSchema;
  }

  public InputStream getRawStream() {
    return this.is;
  }

  public Column[] getSchema() throws IOException {
    if (schemaColumns == null) {
      loadSchema();
    }
    return schemaColumns;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Record read() throws IOException {
    if (schemaColumns == null) {
      loadSchema();
    }
    ArrayRecord ret = new ArrayRecord(schemaColumns);
    String[] data = load();
    if (data == null) {
      return null;
    }
    for (int i = 0; i < data.length; ++i) {
      try {
        setStringByType(ret, i, data[i]);
      } catch (Exception e) {
        throw new IOException("Parse Error while trans value", e);
      }
    }
    return ret;
  }

  public List<String> readRaw() throws IOException {
    if (schemaColumns == null) {
      loadSchema();
    }
    String[] data = load();
    if (data == null) {
      return null;
    }
    return Arrays.asList(data);

  }

  private DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

  private void setStringByType(ArrayRecord ret, int idx, String st) throws ParseException {
    if (st == null) {
      ret.set(idx, null);
      return;
    }
    switch (ret.getColumns()[idx].getTypeInfo().getOdpsType()) {
      case BIGINT:
        ret.setBigint(idx, Long.valueOf(st));
        break;
      case DOUBLE:
        if ("nan".equals(st)) {
          ret.setDouble(idx, Double.NaN);
        } else if ("inf".equals(st)) {
          ret.setDouble(idx, Double.POSITIVE_INFINITY);
        } else if ("-inf".equals(st)) {
          ret.setDouble(idx, Double.NEGATIVE_INFINITY);
        } else {
          ret.setDouble(idx, Double.valueOf(st));
        }
        break;
      case BOOLEAN:
        ret.setBoolean(idx, Boolean.valueOf(st));
        break;
      case DATETIME:
        ret.setDatetime(idx, dateTimeFormat.parse(st));
        break;
      case STRING:
        ret.setString(idx, st);
        break;
      case DECIMAL:
        ret.setDecimal(idx, new BigDecimal(st));
        break;
      case MAP:
        // TODO: support MAP
        ret.set(idx, null);
        break;
      case ARRAY:
        // TODO: support ARRAY
        ret.set(idx, null);
        break;
      case VOID:
        ret.set(idx, null);
        break;
      case TINYINT:
        ret.set(idx, Byte.valueOf(st));
        break;
      case SMALLINT:
        ret.set(idx, Short.valueOf(st));
        break;
      case INT:
        ret.set(idx, Integer.valueOf(st));
        break;
      case FLOAT:
        ret.set(idx, Float.valueOf(st));
        break;
      case CHAR:
        ret.set(idx, new Char(st));
        break;
      case VARCHAR:
        ret.set(idx, new Varchar(st));
        break;
      case DATE:
        ret.setDate(idx, new java.sql.Date(dateFormat.parse(st).getTime()));
        break;
      case TIMESTAMP:
        String[] splits = st.split("\\.");
        if (splits.length > 2) {
          throw new ParseException("Invalid timestamp value", st.lastIndexOf("."));
        }
        java.sql.Timestamp timestamp =
            new java.sql.Timestamp(dateTimeFormat.parse(splits[0]).getTime());
        if (splits.length == 2 && !splits[1].isEmpty()) {
          String nanoValueStr = splits[1];
          // 9 is the max number of digits allowed for a nano value
          if (nanoValueStr.length() > 9) {
            nanoValueStr = nanoValueStr.substring(0, 9);
          } else if (nanoValueStr.length() < 9) {
            StringBuilder nanoValueStrBuilder = new StringBuilder();
            nanoValueStrBuilder.append(nanoValueStr);
            while (nanoValueStrBuilder.length() < 9) {
              nanoValueStrBuilder.append("0");
            }
            nanoValueStr = nanoValueStrBuilder.toString();
          }
          timestamp.setNanos(Integer.parseInt(nanoValueStr));
        }
        ret.setTimestamp(idx, timestamp);
        break;
      case BINARY:
        ret.set(idx, new Binary(st.getBytes()));
        break;
      case INTERVAL_DAY_TIME:
        ret.set(idx, null);
        break;
      case INTERVAL_YEAR_MONTH:
        ret.set(idx, null);
        break;
      case STRUCT:
        // TODO: support STRUCT
        ret.set(idx, null);
        break;
      default:
        throw new RuntimeException("Unsupported type " + ret.getColumns()[idx].getType());
    }
  }

  private void loadSchema() throws IOException {
    String[] schema = load();
    schemaColumns = new Column[schema.length];
    for (int i = 0; i < schemaColumns.length; ++i) {
      String name = schema[i];
      if (tableSchema.containsPartitionColumn(name)) {
        schemaColumns[i] = tableSchema.getPartitionColumn(name);
      } else {
        schemaColumns[i] = tableSchema.getColumn(schema[i]);
      }
    }
  }

  private String[] load() throws IOException {
    String nextLine[] = null;
    if (reader.readRecord()) {
      nextLine = reader.getValues();
      // Why do we use the overload String.split(String, int)?
      // If the input line is "\t", we expected
      // it returns two empty entries, however String.split() return
      // an empty array.
      for (int i = 0; i < nextLine.length; i++) {
        if (nextLine[i].equals(NULL_TOKEN)) {
          nextLine[i] = null;
          continue;
        }
        // XXX ???!!!
        if (trimStringQuotes(nextLine[i]).equals(NULL_TOKEN)) {
          nextLine[i] = nextLine[i].substring(1, nextLine[i].length() - 1);
        }
      }
      return nextLine;
    } else {
      return null;
    }
  }

  private static String trimStringQuotes(String str) {
    int i = 0;
    int len = str.length();
    while (i < len / 2 && str.charAt(i) == QUOTE && str.charAt(len - i - 1) == QUOTE) {
      i++;
    }
    return str.substring(i, str.length() - i);
  }

  @Override
  public void close() {
    reader.close();
  }

}
