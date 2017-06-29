package com.aliyun.odps.io;

import java.io.IOException;

/**
 * Encapsulation class that hosts a collection of output streams.
 **/
public interface OutputStreamSet{
  /**
   * Access method for getting next output stream.
   * @return: SinkOutputStream.
   **/
  SinkOutputStream next() throws IOException;

  /**
   * Get next output stream with specified postfix name.
   * @param fileNamePostfix The postfix for the created file name
   * @return SinkOutputStream
   */
  SinkOutputStream next(String fileNamePostfix) throws IOException;
}
