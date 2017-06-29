package com.aliyun.odps.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;

/**
 * An extension of Java InputStream used to interface with the input file byte stream
 **/
public abstract class SourceInputStream extends InputStream {

  /** Reads some number of bytes from the input stream and stores them into the buffer array b.
   *  The number of bytes actually read is returned as an integer.
   * @param b the buffer into which the data is read
   * @param offset the start offset in array b at which the data is written
   * @param length  the maximum number of bytes to read
   * @return the total number of bytes read into the buffer, or -1 if there is no more data
   * @throws IOException
   */
  @Override
  public abstract int read(byte[] b, int offset, int length) throws IOException;

  /** Reads some number of bytes from the input stream and stores them into the buffer array b.
   *  The number of bytes actually read is returned as an integer.
   * @param b the buffer into which the data is read
   * @return the total number of bytes read into the buffer, or -1 if there is no more data
   * @throws IOException
   */
  @Override
  public abstract int read(byte[] b) throws IOException;

  /**
   * Read one byte from stream. This method is very inefficient. Use
   * read(byte[], int, int) instead.
   * @return The byte value read. -1 if no more data.
   * @throws IOException
   */
  @Override
  public abstract int read() throws IOException;

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public int available() throws IOException {
    return super.available();
  }

  @Override
  public void mark(int readLimit){
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean markSupported(){
    return false;
  }

  @Override
  public void reset(){
    throw new UnsupportedOperationException();
  }

  @Override
  public abstract  long skip(long n) throws IOException;

  /**
   * Getter for the file name associated with the file being streamed in.
   * @return: file name
   **/
  public abstract String getFileName();

  /**
   * Getter for the size in [bytes] of the file currently being processed.
   **/
  public abstract long getFileSize();

  /**
   * An attempt to read rest of file content from current position (init to begin of file) to the end of file,
   * into the supplied buffer. The supplied byte buffer will be first checked to ensure that it is large enough
   * to hold read-out content, and may throw BufferOverflowException if the checks fail, before actual reading.
   * Once successful, the read-out byte count will be returned.
   * This could be used to read an entire file of less than Integer.MAX_VALUE bytes, for files larger than
   * such limit, use a series of read(byte[] buffer, int offset, int length) instead.
   * @param buffer: the byte buffer to host read-out bytes in a successful read
   * @return: number of read-out bytes after a successful read
   **/
  public abstract int readToEnd(byte[] buffer) throws IOException, BufferOverflowException;

  /**
   * Clone a stream, the clone is a separate handle to the source data. This allows flexible data manipulations,
   * such as operations on different segments of the source files, without frequent seek/reset operations.
   * @return cloned stream
   * @throws IOException
   */
  public abstract SourceInputStream cloneStream() throws IOException;

  /**
   * Allow adjustment on the upper limit for number of maximum cloned streams allowed.
   * The system will set a reasonable upper limit by default, so unless absolutely necessary,
   * it is NOT recommended that user adjusts this value.
   * @param limit
   */
  public abstract void adjustMaxCloneLimit(int limit);

}
