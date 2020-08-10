package com.aliyun.odps.udf.local.examples;

import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve("*->string")
public class UdafVarLengthArg extends Aggregator {

  @Override
  public void iterate(Writable buffer, Writable[] args) {
    Text result = (Text) buffer;
    for (Writable item : args) {
      if (item != null) {
        result.set(result.toString() + item.toString());
      }
    }
  }

  @Override
  public void merge(Writable buffer, Writable partial) {
    Text result = (Text) buffer;
    Text partialResult = (Text) partial;
    result.set(result.toString() + partialResult.toString());
  }

  @Override
  public Writable newBuffer() {
    return new Text();
  }

  @Override
  public Writable terminate(Writable buffer) {
    return buffer;
  }

}