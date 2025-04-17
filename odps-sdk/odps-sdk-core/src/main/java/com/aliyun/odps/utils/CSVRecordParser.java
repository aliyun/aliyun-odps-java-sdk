package com.aliyun.odps.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.converter.OdpsRecordConverter;
import com.aliyun.odps.data.converter.OdpsRecordConverterBuilder;
import com.csvreader.CsvReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by dongxiao on 2020/3/25.
 */
public class CSVRecordParser {

  public static class ParseResult {
    TableSchema schema = null;
    List<Record> records = null;

    public ParseResult(TableSchema schema, List<Record> records) {
      this.schema = schema;
      this.records = records;
    }

    public TableSchema getSchema() {
      return schema;
    }

    public List<Record> getRecords() {
      return records;
    }
  }

  public static ParseResult parse(String csvString) throws OdpsException {
    CsvReader reader = new CsvReader(new StringReader(csvString));
    reader.setSafetySwitch(false);
    int lineCount = 0;
    String[] newline;
    Column[] columns = new Column[]{};

    List<Record> records = new ArrayList<Record>();
    TableSchema schema = new TableSchema();

    try {
      while (reader.readRecord()) {
        newline = reader.getValues();
        // the first line is column names
        if (lineCount == 0) {
          columns = new Column[newline.length];
          for (int i = 0; i < newline.length; i++) {
            columns[i] = new Column(newline[i], OdpsType.STRING);
            schema.addColumn(columns[i]);
          }
        } else {
          Record record = new ArrayRecord(columns);
          for (int i = 0; i < newline.length; i++) {
            record.set(i, newline[i]);
          }
          records.add(record);
        }
        lineCount++;
      }
    } catch (IOException e) {
      throw new OdpsException("Error when parse sql results.", e);
    }
    return new ParseResult(schema, records);
  }

  private static final OdpsRecordConverter formatter;
  static {
    // TODO: timezone
    formatter = OdpsRecordConverter.builder()
        .enableParseNull()
        .nullFormat("\\N")
        .binaryFormatUtf8()
        .build();
  }

  public static ParseResult parse(String csvString, TableSchema schema) throws OdpsException {
    if (schema == null) {
      return parse(csvString);
    }
    CsvReader reader = new CsvReader(new StringReader(csvString));
    reader.setSafetySwitch(false);
    int lineCount = 0;
    String[] newline;
    List<Column> columns = schema.getColumns();
    List<Record> records = new ArrayList<>();
    try {
      while (reader.readRecord()) {
        newline = reader.getValues();
        // the first line is column names
        if (lineCount == 0) {
          if (newline.length != columns.size()) {
            throw new OdpsException(
                "Result column count not match result descriptor's schema count. " + newline.length
                + " != " + columns.size());
          }
        } else {
          Record record = new ArrayRecord(schema);
          for (int i = 0; i < newline.length; i++) {
            try {
              record.set(i, formatter.parseObject(newline[i], columns.get(i).getTypeInfo()));
            } catch (Exception ignored) {
              record.set(i, null);
            }
          }
          records.add(record);
        }
        lineCount++;
      }
    } catch (IOException e) {
      throw new OdpsException("Error when parse sql results.", e);
    }
    return new ParseResult(schema, records);
  }
}
