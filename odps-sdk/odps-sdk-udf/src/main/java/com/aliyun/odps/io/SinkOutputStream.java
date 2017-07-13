package com.aliyun.odps.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An extension of Java OutputStream, used to interface with the output file byte stream.
 **/
public abstract class SinkOutputStream extends OutputStream {

  /**
   * output stream shall be closed by the system AFTER Outputer::close()
   * calling close() on SinkOutputStream is a no-op
   **/
  @Override
  public final void close() {
    // no-op
  }

  /**
   * Note that system will be performing memory control in the background, and will flush data only
   * when it is deemed necessary, therefore the flush here would be no-op.
   **/
  @Override
  public final void flush() {
    // no-op
  }

  /**
   * Writes b.length bytes from the specified byte array to this output stream
   * @param b the data.
   * @throws IOException
   */
  @Override
  public abstract void write(byte[] b) throws IOException;

  /**
   * Writes len bytes from the specified byte array starting at offset off to this output stream.
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @throws IOException
   */
  @Override
  public abstract void write(byte[] b, int off, int len)throws IOException;

  /**
   * Writes a single byte to the output stream on each invocation, which is very
   * INEFFICIENT, it is not recommend to use this unless it is desirable to write one single
   * byte on each call. Use write(byte[]) or write(byte[], int, int) instead.
   */
  @Override
  public abstract void write(int b)throws IOException;
}
