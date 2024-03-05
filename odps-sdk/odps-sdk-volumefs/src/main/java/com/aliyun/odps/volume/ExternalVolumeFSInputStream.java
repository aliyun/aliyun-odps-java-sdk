package com.aliyun.odps.volume;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;

import com.aliyun.odps.fs.VolumeFileSystemConfigKeys;
import com.aliyun.odps.volume.protocol.VolumeFSConstants;

/**
 * @author dingxin
 */
public class ExternalVolumeFSInputStream extends FSInputStream {

  private FileInputStream input;
  private long position;
  private File tempFile;

  public ExternalVolumeFSInputStream(String path, VolumeFSClient volumeClient, Configuration conf)
      throws IOException {
    String uuid = UUID.randomUUID().toString();
    File
        bufferBlockDir =
        new File(conf.get(VolumeFileSystemConfigKeys.ODPS_VOLUME_BLOCK_BUFFER_DIR,
                          VolumeFSConstants.DEFAULT_VOLUME_BLOCK_BUFFER_DIR));
    if (!bufferBlockDir.exists() && !bufferBlockDir.mkdirs()) {
      throw new IOException("Cannot create Volume block buffer directory: " + bufferBlockDir);
    }
    String tempFileName = bufferBlockDir.getPath() + uuid;
    try {
      volumeClient.downloadFileFromExternalVolume(path, bufferBlockDir.getPath() + uuid);
    } catch (Exception e) {
      throw new IOException("download " + path + " error! ", e);
    }
    tempFile = new File(tempFileName);
    input = new FileInputStream(tempFile);
  }

  @Override
  public int read() throws IOException {
    int result = input.read();
    if (result != -1) {
      position++;
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    input.close();
    tempFile.delete();
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IllegalArgumentException("Position cannot be negative");
    }
    if (pos > input.getChannel().size()) {
      throw new IllegalArgumentException("Position cannot be greater than the file size");
    }
    position = pos;
    input.getChannel().position(pos);
  }

  @Override
  public long getPos() {
    return position;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    if (targetPos < 0) {
      throw new IllegalArgumentException("Target position cannot be negative");
    }
    if (targetPos > input.getChannel().size()) {
      throw new IllegalArgumentException("Target position cannot be greater than the file size");
    }
    long oldPos = position;
    try {
      input.getChannel().position(targetPos);
      position = targetPos;
      return true;
    } catch (IOException e) {
      input.getChannel().position(oldPos);
      position = oldPos;
      return false;
    }
  }
}

