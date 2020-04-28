package com.aliyun.odps.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.csvreader.CsvReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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
}
