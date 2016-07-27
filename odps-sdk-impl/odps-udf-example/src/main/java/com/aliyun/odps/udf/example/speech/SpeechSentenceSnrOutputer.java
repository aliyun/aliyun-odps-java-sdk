package com.aliyun.odps.udf.example.speech;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.SinkOutputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Outputer;

public class SpeechSentenceSnrOutputer extends Outputer {

  public SpeechSentenceSnrOutputer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setup(ExecutionContext ctx, SinkOutputStream outputStream, DataAttributes attributes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void output(Record record) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }
}
