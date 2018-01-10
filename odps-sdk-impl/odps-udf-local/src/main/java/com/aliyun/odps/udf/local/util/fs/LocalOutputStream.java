package com.aliyun.odps.udf.local.util.fs;

import com.aliyun.odps.io.SinkOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalOutputStream extends SinkOutputStream {
  private FileOutputStream fileOutputStream;
  private long bytesWritten;
  private boolean finalized;
  public LocalOutputStream(File file) throws IOException {
    // if file already exists this will return false (which we ignore)
    file.createNewFile();
    this.fileOutputStream = new FileOutputStream(file);
    this.bytesWritten = 0;
    this.finalized = false;
    System.out.println("Output file created at " + file.getPath());
  }
  @Override
  public void write(byte[] b) throws IOException {
    this.fileOutputStream.write(b);
    this.bytesWritten += b.length;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    this.fileOutputStream.write(b, off, len);
    this.bytesWritten += len;
  }

  @Override
  public void write(int b) throws IOException {
    this.fileOutputStream.write(b);
    this.bytesWritten++;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  // since SinkOutputStream does not allow overriding of flush/close, this is implemented to replace that
  // on local file stream
  public void finalize() throws IOException {
    if (!this.finalized) {
      this.fileOutputStream.flush();
      this.fileOutputStream.close();
      this.finalized = true;
    }
  }
}
