package com.aliyun.odps.exec;

/**
 *
 */

import java.io.IOException;

/**
 * <code>InputSplit</code> represents the data to be processed by an
 * individual mapper.
 *
 * <p>Typically, it presents a byte-oriented view on the input and is the
 * responsibility of {@link RecordReader} of the job to process this and present
 * a record-oriented view.
 *
 * @see InputFormat
 */
public abstract class InputSplit {

  /**
   * Get the total number of bytes in the data of the <code>InputSplit</code>.
   *
   * @return the number of bytes in the input split.
   * @throws IOException
   */
  public abstract long getLength() throws IOException;

  /**
   * Get the list of hostnames where the input split is located.
   *
   * @return list of hostnames where data of the <code>InputSplit</code> is
   *         located as an array of <code>String</code>s.
   * @throws IOException
   */
  public abstract String[] getLocations() throws IOException;
}