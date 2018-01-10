package com.aliyun.odps.udf.test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.OutputStreamSet;
import com.aliyun.odps.udf.*;

import java.io.IOException;

/**
 * Test 'hdfs://' scheme used as customized location
 */
public class CustomizedHDFSStorageHandler extends OdpsStorageHandler {

  public static class HDFSOutputer extends Outputer {

    @Override
    public void setup(ExecutionContext ctx, OutputStreamSet outputStreamSet, DataAttributes attributes) throws IOException {
      String locationInAttrs = attributes.getValueByKey("location");
      String location = attributes.getCustomizedDataLocation();
      if (!location.equals(locationInAttrs)) {
        throw new IllegalArgumentException("Expected location: " + locationInAttrs + " but got: " + location);
      }
    }

    @Override
    public void output(Record record) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
  }

  @Override
  public Class<? extends Extractor> getExtractorClass() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Class<? extends Outputer> getOutputerClass() {
    return HDFSOutputer.class;
  }
}
