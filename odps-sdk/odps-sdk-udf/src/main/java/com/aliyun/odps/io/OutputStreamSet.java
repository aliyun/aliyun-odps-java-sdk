package com.aliyun.odps.io;

import java.io.IOException;

/**
 * Encapsulation class that hosts a collection of output streams.
 **/
public interface OutputStreamSet{
  /**
   * Access method for getting next output stream.
   * @return: SinkOutputStream or null
   * Note: For customized external storage location (other than built-in supported, such as Aliyun OSS)
   *       null will returned and user will be solely responsible for interacting with external storage
   **/
  SinkOutputStream next() throws IOException;

  /**
   * Get next output stream with specified postfix name.
   * @param fileNamePostfix The postfix for the created file name
   * @return SinkOutputStream or null
   * Note: when a customized external storage location (other than Aliyun OSS) is provided,
   *       null will returned and user will be solely responsible for interacting with external storage
   */
  SinkOutputStream next(String fileNamePostfix) throws IOException;
}
