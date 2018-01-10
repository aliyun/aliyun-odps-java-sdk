package com.aliyun.odps.udf.local.util.fs;

import com.aliyun.odps.io.SourceInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;

public class LocalInputStream extends SourceInputStream{
  private FileInputStream fileInputStream;
  private File file;
  private long currentPos;

  public LocalInputStream(File file) throws IOException{
    this.file = file;
    this.fileInputStream = new FileInputStream(file);
    this.currentPos = 0;
  }
  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    int read = this.fileInputStream.read(b, offset, length);
    this.currentPos += read;
    return read;
  }

  @Override
  public int read(byte[] b) throws IOException {
    int read = this.fileInputStream.read(b);
    this.currentPos += read;
    return read;
  }

  @Override
  public int read() throws IOException {
    int read = this.fileInputStream.read();
    if (read > 0) {
      this.currentPos++;
    }
    return read;
  }

  @Override
  public long skip(long n) throws IOException {
    long s = fileInputStream.skip(n);
    if (s > 0) {
      this.currentPos += s;
    }
    return s;
  }

  @Override
  public String getFileName() {
    return this.file.getName();
  }

  @Override
  public long getFileSize() {
    return this.file.length();
  }

  @Override
  public long getSplitStart() {
    return 0;
  }

  // for now no in-file split is supported for local files
  @Override
  public long getSplitSize() {
    return this.file.length();
  }

  @Override
  public long getCurrentPos() throws IOException {
    return this.currentPos;
  }

  @Override
  public int readToEnd(byte[] buffer) throws IOException, BufferOverflowException {
    long sizeToRead = getSplitSize();
    if (buffer.length < sizeToRead) {
      throw new BufferOverflowException();
    }
    if (sizeToRead> Integer.MAX_VALUE) {
      throw new
          IllegalArgumentException("readToEnd does not support size larger than Integer.MAX_VALUE." +
          "Please use multiple read(byte[] buffer, int offset, int length) to read large files.");
    }
    int readSize = this.fileInputStream.read(buffer, 0, (int)sizeToRead);
    return readSize;
  }

  @Override
  public SourceInputStream cloneStream() throws IOException {
    return new LocalInputStream(this.file);
  }

  @Override
  public void adjustMaxCloneLimit(int limit) {
    throw new UnsupportedOperationException("no need to adjust max clone limit for local file.");
  }

  @Override
  public long getBytesRead() {
    throw new UnsupportedOperationException("getBytesRead not supported for local file.");
  }
}
