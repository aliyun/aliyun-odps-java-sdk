package com.aliyun.odps.udf.example.filesoutput;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.OutputStreamSet;
import com.aliyun.odps.io.SinkOutputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Outputer;

import java.io.IOException;
import java.util.Random;

/**
 *
 */
public class MultiFileOutputer extends Outputer {
  private OutputStreamSet outputStreamSet;
  private DataAttributes attributes;
  private String delimiter;
  private SinkOutputStream stream;
  private int recordCount;
  private boolean simulateRandomFail = false;
  private int recordsPerFile = 500;

  public MultiFileOutputer (){
    // default delimiter, this can be overwritten if a delimiter is provided through the attributes.
    this.delimiter = "|";
    this.recordCount = 0;
  }

  @Override
  public void setup(ExecutionContext ctx, OutputStreamSet outputStreamSet, DataAttributes attributes) {
    this.outputStreamSet = outputStreamSet;
    this.attributes = attributes;
    simulateRandomFail = attributes.getValueByKey("simulate.random.fail") != null;
    String recordsPerFileStr = attributes.getValueByKey("records.per.file");
    if (recordsPerFileStr != null) {
      recordsPerFile = Integer.valueOf(recordsPerFileStr);
    }
  }

  @Override
  public void output(Record record) throws IOException {
    // each file contains at most 1000 records, note that this usage is only for demo purpose
    if (recordCount % recordsPerFile == 0) {
      stream = outputStreamSet.next("custom_postfix_" + recordCount + ".txt");
    }
    recordCount++;
    stream.write(recordToString(record).getBytes());
  }

  @Override
  public void close() throws IOException {
    if (simulateRandomFail) {
      randomFail();
    }
  }

  private void randomFail() throws IOException {
    Random random = new Random();
    if (random.nextBoolean()) {
      throw new IOException();
    }
  }

  private String recordToString(Record record){
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < record.getColumnCount(); i++)
    {
      if (null == record.get(i)){
        sb.append("NULL");
      }
      else{
        sb.append(record.get(i).toString());
      }
      if (i != record.getColumnCount() - 1){
        sb.append(this.delimiter);
      }
    }
    sb.append("\n");
    return sb.toString();
  }
}
