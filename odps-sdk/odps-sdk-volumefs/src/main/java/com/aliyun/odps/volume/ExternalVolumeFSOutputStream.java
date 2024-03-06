package com.aliyun.odps.volume;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.aliyun.odps.fs.VolumeFileSystemConfigKeys;
import com.aliyun.odps.volume.protocol.VolumeFSConstants;

public class ExternalVolumeFSOutputStream extends OutputStream {
  private String path;
  private File file;
  private FileOutputStream outputStream;
  private VolumeFSClient volumeClient;

  public ExternalVolumeFSOutputStream(String path, VolumeFSClient volumeClient,
                                      boolean overwrite, Configuration conf) throws IOException {
    this.volumeClient = volumeClient;
    this.path = path;
    File
        bufferBlockDir =
        new File(conf.get(VolumeFileSystemConfigKeys.ODPS_VOLUME_BLOCK_BUFFER_DIR,
                          VolumeFSConstants.DEFAULT_VOLUME_BLOCK_BUFFER_DIR));
    if (!bufferBlockDir.exists() && !bufferBlockDir.mkdirs()) {
      throw new IOException("Cannot create Volume block buffer directory: " + bufferBlockDir);
    }
    String uuid = UUID.randomUUID().toString();
    String tempFileName = bufferBlockDir.getPath() + uuid;
    if (!overwrite) {
      try {
        volumeClient.downloadFileFromExternalVolume(path, tempFileName);
      } catch (Exception ignored) {
      }
    }
    file = new File(tempFileName);
    outputStream = new FileOutputStream(file, true);
  }

  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    outputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    outputStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
    try {
      volumeClient.uploadFileToExternalVolume(path, Files.newInputStream(file.toPath()));
    } catch (Exception e) {
      throw new IOException("upload file " + path + "failed!", e);
    } finally {
      file.delete();
    }
  }
}