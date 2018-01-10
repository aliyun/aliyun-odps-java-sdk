package com.aliyun.odps.udf.local.util.fs;

import com.aliyun.odps.io.InputStreamSet;
import com.aliyun.odps.io.SourceInputStream;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class LocalInputStreamSet implements InputStreamSet {
  List<File> files;
  int fileIndex;
  public LocalInputStreamSet(List<File> files) {
    this.files = files;
    this.fileIndex = 0;
  }
  @Override
  public SourceInputStream next() throws IOException {
    if (this.fileIndex == this.files.size()) {
      return null;
    }
    return new LocalInputStream(this.files.get(this.fileIndex++));
  }
}
