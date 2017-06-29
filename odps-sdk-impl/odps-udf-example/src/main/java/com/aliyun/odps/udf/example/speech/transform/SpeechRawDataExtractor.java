package com.aliyun.odps.udf.example.speech.transform;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.InputStreamSet;
import com.aliyun.odps.io.SourceInputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Extractor;

import java.io.IOException;

public class SpeechRawDataExtractor extends Extractor {
  private InputStreamSet inputs;
  private DataAttributes attributes;
  private Column[] outputColumns;
  @Override
  public void setup(ExecutionContext ctx, InputStreamSet inputs, DataAttributes attributes) {
    this.inputs = inputs;
    this.attributes = attributes;
    this.outputColumns = attributes.getRecordColumns();

    OdpsType expectedSchemas [] = new OdpsType[] {OdpsType.STRING, OdpsType.BIGINT, OdpsType.BINARY};
    this.attributes.verifySchema(expectedSchemas);
  }

  @Override
    public Record extract() throws IOException {
    SourceInputStream inputStream = inputs.next();
    if (inputStream == null){
      return null;
    }

    String fileName = inputStream.getFileName();
    fileName = fileName.substring(fileName.lastIndexOf('/') + 1);
    System.out.println("Processing wav file " + fileName);
    String id = fileName.substring(0, fileName.lastIndexOf('.'));
    byte[] content = new byte[(int)inputStream.getFileSize()];
    inputStream.readToEnd(content);
    ArrayRecord record = new ArrayRecord(this.outputColumns);
    record.setString(0, id);
    record.setBigint(1, inputStream.getFileSize());
    record.setBinary(2, new Binary(content));
    return record;
  }

  @Override
  public void close() {
  }
}
