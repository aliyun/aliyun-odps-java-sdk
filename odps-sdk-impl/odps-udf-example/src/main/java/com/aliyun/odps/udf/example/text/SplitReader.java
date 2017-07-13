package com.aliyun.odps.udf.example.text;

import java.io.IOException;
import java.io.Reader;

/**
 * A reader that reads files split, it encapsulates an internal Reader, and wraps around the read operation
 * with a sanity check whether the read has reached the split end: when it does it stop reading and return -1.
 */
public class SplitReader extends Reader {
  private Reader internalReader;
  private long size;
  private long currentPos;

  public SplitReader(Reader reader, long splitSize) {
    this.internalReader = reader;
    this.size = splitSize;
    this.currentPos = 0;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    if (this.currentPos >= this.size){
      return -1;
    }
    this.currentPos ++;
    return this.internalReader.read(cbuf, off, len);
  }

  @Override
  public void close() throws IOException {
    this.internalReader.close();
  }
}
