package com.aliyun.odps.udf;

import com.aliyun.odps.io.Writable;

import java.io.IOException;

/**
 * RecordWriter writes the output Writable record (usually serialized by an implementation of
 * {@link RecordSerDe})to output.
 */
public interface RecordWriter {
  /**
   *
   * @param value Writes a Writable record
   * @throws IOException
   */
  void write(Writable value) throws IOException;

  void close() throws IOException;
}
