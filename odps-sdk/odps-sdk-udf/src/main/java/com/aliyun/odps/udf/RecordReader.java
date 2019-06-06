package com.aliyun.odps.udf;

import com.aliyun.odps.io.Writable;

import java.io.IOException;

/**
 * RecordReader converts the byte-oriented view of the input, provided by the InputSplit,
 * and presents a record-oriented Writable (which will usually de-serialized into {@link com.aliyun.odps.data.Record}
 * by an implementation of {@link RecordSerDe} ). It thus assumes the responsibility of
 * processing record boundaries and presenting the tasks with record Writable
 */
public interface RecordReader {
  /**
   * Create an Writable of the appropriate type to be used as a record.
   * @return a new Writable object.
   */
   Writable createRecord();


  /**
   * Reads the next record as Writable from the input for processing.
   * @param record the Writable to read data into
   * @return true iff a record was read, false if at EOF
   * @throws IOException
   */
  boolean next(Writable record) throws IOException;

  /**
   *
   */
  void close() throws IOException;
}

