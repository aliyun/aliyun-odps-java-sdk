package com.aliyun.odps.udf.example.speech.transform;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.OutputStreamSet;
import com.aliyun.odps.io.SinkOutputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Outputer;

import java.io.IOException;

public class SpeechRawDataOutputer extends Outputer {
  private OutputStreamSet outputStreamSet;
  private DataAttributes attributes;

  @Override
  public void setup(ExecutionContext ctx, OutputStreamSet outputStreamSet, DataAttributes attributes) {
    this.outputStreamSet = outputStreamSet;
    this.attributes = attributes;
    this.attributes.verifySchema(new OdpsType[]{ OdpsType.STRING, OdpsType.BINARY });
  }

  @Override
  public void output(Record record) throws IOException {
    String id = record.getString(0);
    SinkOutputStream stream = this.outputStreamSet.next(id + ".wav");
    stream.write(record.getBytes(1));
  }

  @Override
  public void close() {

  }
}
