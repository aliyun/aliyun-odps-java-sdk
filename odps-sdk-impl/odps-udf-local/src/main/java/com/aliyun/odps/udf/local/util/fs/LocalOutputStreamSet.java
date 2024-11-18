package com.aliyun.odps.udf.local.util.fs;

import com.aliyun.odps.io.OutputStreamSet;
import com.aliyun.odps.io.SinkOutputStream;
import org.apache.commons.io.FilenameUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class LocalOutputStreamSet implements OutputStreamSet, Closeable {
  private File directory;
  Set<LocalOutputStream> outputStreams;
  public LocalOutputStreamSet(String path) throws IOException{
    this.directory = new File(path);
    if (!this.directory.exists()) {
      this.directory.mkdirs();
    }
    this.outputStreams = new HashSet<LocalOutputStream>();
  }
  @Override
  public SinkOutputStream next() throws IOException {
    return next("");
  }

  @Override
  public SinkOutputStream next(String fileNamePostfix) throws IOException {
    String fileName = UUID.randomUUID().toString().substring(0,8);
    LocalOutputStream stream =
        new LocalOutputStream(new File(FilenameUtils.concat(this.directory.getAbsolutePath(), fileName)));
    this.outputStreams.add(stream);
    return stream;
  }

  public void close() throws IOException{
    for (LocalOutputStream stream : outputStreams){
      stream.finalize();
    }
  }
}
