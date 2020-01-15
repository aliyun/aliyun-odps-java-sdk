package com.aliyun.odps.udf;


import com.aliyun.odps.data.Record;

import java.io.Closeable;
import java.io.IOException;


public interface TableRecordReader extends Closeable {

  boolean hasNext() throws IOException;

  Record next() throws IOException;

  @Override
  void close() throws IOException;

}
