/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aliyun.odps.volume;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Volume;
import com.aliyun.odps.VolumeException;
import com.aliyun.odps.VolumeFSFile;
import com.aliyun.odps.Volumes;
import com.aliyun.odps.fs.VolumeFileSystemConfigKeys;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.VolumeFSErrorCode;
import com.aliyun.odps.tunnel.VolumeFSTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.VolumeOutputStream;
import com.aliyun.odps.volume.protocol.VolumeFSConstants;
import com.aliyun.odps.volume.protocol.VolumeFSErrorMessageGenerator;

/**
 * Client for Volume
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFSClient {

  private Odps odps;

  private String tunnelEndpoint;

  private Configuration conf;

  public VolumeFSClient(Odps odps, String project, String serviceEndpoint, String tunnelEndpoint,
      Configuration conf) {
    this.odps = odps;
    this.odps.setDefaultProject(project);
    this.odps.setEndpoint(serviceEndpoint);
    this.tunnelEndpoint = tunnelEndpoint;
    this.conf = conf;
  }

  /**
   * Get File meta info
   * 
   * @param path
   * @throws VolumeException
   */
  public VolumeFSFile getFileInfo(String path) throws VolumeException {

    return new VolumeFSJobRunnerProxy<VolumeFSFile>() {

      @Override
      public VolumeFSFile doJob(String path, Map<String, Object> params) throws VolumeException {

        VolumeFSFile file =
            odps.volumes().get(VolumeFSUtil.getVolumeFromPath(path)).getVolumeFSFile(path);
        try {
          file.reload();
        } catch (OdpsException e) {
          throw new VolumeException(e);
        }
        return file;
      }

      @Override
      public VolumeFSFile onVolumeMissing(String path, Map<String, Object> params)
          throws VolumeException {
        VolumeFSFile file = VolumeFSFile.getRoot(odps.getDefaultProject(), odps);
        return file;
      }

      @Override
      public VolumeFSFile onNoSuchVolume(String path, Map<String, Object> params)
          throws VolumeException {
        throw new VolumeException(VolumeFSErrorCode.NoSuchPath,
            VolumeFSErrorMessageGenerator.noSuchFileOrDirectory(path));
      }

      @Override
      public VolumeFSFile onInvalidPath(String path, Map<String, Object> params)
          throws VolumeException {
        if (VolumeFSUtil.checkPathIsJustVolume(path)) {
          try {
            if (!odps.volumes().exists(VolumeFSUtil.getVolumeFromPath(path))) {
              throw new VolumeException(VolumeFSErrorCode.NoSuchPath,
                  VolumeFSErrorMessageGenerator.noSuchFileOrDirectory(path));
            }
          } catch (OdpsException e) {
            throw new VolumeException(e);
          }
          Volume v = odps.volumes().get(VolumeFSUtil.getVolumeFromPath(path));
          if (isNewVolume(v)) {
            return VolumeFSFile.transferVolumeToVolumeFSFile(odps.getDefaultProject(), v,
                odps.getRestClient());
          } else {
            throw new VolumeException(VolumeFSErrorCode.NoSuchPath,
                VolumeFSErrorMessageGenerator.oldVolumeAlert(VolumeFSUtil.getVolumeFromPath(path)));
          }
        } else {
          throw (VolumeException) params.get(CUR_EXCEPTION);
        }
      }

    }.run(path, null);

  }

  /**
   * Make directory
   * 
   * @param path
   * @throws VolumeException
   */
  public boolean mkdirs(String path) throws VolumeException {

    return new VolumeFSJobRunnerProxy<Boolean>() {

      @Override
      public Boolean doJob(String path, Map<String, Object> params) throws VolumeException {

        VolumeFSFile.create(odps.getDefaultProject(), path, true, odps.getRestClient());
        return true;
      }

      @Override
      public Boolean onVolumeMissing(String path, Map<String, Object> params)
          throws VolumeException {
        throw new VolumeException(VolumeFSErrorCode.PathAlreadyExists,
            VolumeFSErrorMessageGenerator.fileExists(path));
      }

      @Override
      public Boolean onNoSuchVolume(String path, Map<String, Object> params) throws VolumeException {

        try {
          odps.volumes().create(VolumeFSUtil.getVolumeFromPath(path),
              VolumeFSConstants.CREATE_VOLUME_COMMENT, Volume.Type.NEW);
        } catch (Exception e) {
          throw new VolumeException(e);
        }
        if (VolumeFSUtil.checkPathIsJustVolume(path)) {
          return true;
        } else {
          return doJob(path, params);
        }
      }

      @Override
      public Boolean onInvalidPath(String path, Map<String, Object> params) throws VolumeException {
        // If the path is a volume name , will return true
        if (VolumeFSUtil.checkPathIsJustVolume(path)) {
          try {
            if (!odps.volumes().exists(VolumeFSUtil.getVolumeFromPath(path))) {
              odps.volumes().create(VolumeFSUtil.getVolumeFromPath(path),
                  VolumeFSConstants.CREATE_VOLUME_COMMENT, Volume.Type.NEW);
            }
          } catch (OdpsException e) {
            throw new VolumeException(e);
          }
          return true;
        } else {
          throw (VolumeException) params.get(CUR_EXCEPTION);
        }
      }

      @Override
      public Boolean onPathAlreadyExists(String path, Map<String, Object> params)
          throws VolumeException {
        VolumeFSFile file =
            odps.volumes().get(VolumeFSUtil.getVolumeFromPath(path)).getVolumeFSFile(path);
        try {
          file.reload();
        } catch (OdpsException oe) {
          throw new VolumeException(oe);
        }
        if (Boolean.TRUE.equals(file.getIsdir())) {
          return Boolean.TRUE;
        } else {
          throw (VolumeException) params.get(CUR_EXCEPTION);
        }
      }

    }.run(path, null);
  }

  /**
   * Set file's replication
   * 
   * @param path
   * @param replication
   * @throws VolumeException
   */
  public boolean setReplication(String path, short replication) throws VolumeException {

    Map<String, Object> params = new HashMap<String, Object>();
    Map<String, String> innerParams = new HashMap<String, String>();

    innerParams.put(VolumeFSFile.ParamKey.REPLICATION.name().toLowerCase(),
        String.valueOf(replication));
    params.put("params", innerParams);

    return new VolumeFSJobRunnerProxy<Boolean>() {

      @SuppressWarnings("unchecked")
      @Override
      public Boolean doJob(String path, Map<String, Object> params) throws VolumeException {

        VolumeFSFile file =
            odps.volumes().get(VolumeFSUtil.getVolumeFromPath(path)).getVolumeFSFile(path);
        file.update((Map<String, String>) params.get("params"));
        return true;
      }

      @Override
      public Boolean onVolumeMissing(String path, Map<String, Object> params)
          throws VolumeException {
        throw new VolumeException(VolumeFSErrorCode.NotAcceptableOperation,
            VolumeFSErrorMessageGenerator.isADirectory(path));
      }

      @Override
      public Boolean onNoSuchVolume(String path, Map<String, Object> params) throws VolumeException {
        throw new VolumeException(VolumeFSErrorCode.NoSuchPath,
            VolumeFSErrorMessageGenerator.noSuchFileOrDirectory(path));
      }
    }.run(path, params);

  }

  /**
   * List files (and directories) in specific path
   * 
   * @param path
   * @throws VolumeException
   */
  public VolumeFSFile[] getFileInfosByPath(String path) throws VolumeException {

    return new VolumeFSJobRunnerProxy<VolumeFSFile[]>() {

      @Override
      public VolumeFSFile[] doJob(String path, Map<String, Object> params) throws VolumeException {

        VolumeFSFile file =
            odps.volumes().get(VolumeFSUtil.getVolumeFromPath(path)).getVolumeFSFile(path);
        Iterator<VolumeFSFile> iterator = file.iterator();
        List<VolumeFSFile> files = new ArrayList<VolumeFSFile>();
        try {
          while (iterator.hasNext()) {
            files.add(iterator.next());
          }
        } catch (RuntimeException e) {
          if (e.getCause() instanceof VolumeException) {
            throw (VolumeException) e.getCause();
          } else {
            throw new VolumeException(e);
          }
        }
        return files.toArray(new VolumeFSFile[0]);
      }

      @Override
      public VolumeFSFile[] onVolumeMissing(String path, Map<String, Object> params)
          throws VolumeException {
        List<VolumeFSFile> files = new ArrayList<VolumeFSFile>();
        Volumes volumes = odps.volumes();
        Iterator<Volume> it = volumes.iterator();
        while (it.hasNext()) {
          Volume v = it.next();
          if (isNewVolume(v)) {
            files.add(VolumeFSFile.transferVolumeToVolumeFSFile(odps.getDefaultProject(), v,
                odps.getRestClient()));
          }
        }
        return files.toArray(new VolumeFSFile[0]);
      }

      @Override
      public VolumeFSFile[] onNoSuchVolume(String path, Map<String, Object> params)
          throws VolumeException {
        throw new VolumeException(VolumeFSErrorCode.NoSuchPath,
            VolumeFSErrorMessageGenerator.noSuchFileOrDirectory(path));
      }

      @Override
      public VolumeFSFile[] onInvalidPath(String path, Map<String, Object> params)
          throws VolumeException {
        if (VolumeFSUtil.checkPathIsJustVolume(path)) {
          return doJob(path, params);
        } else {
          throw (VolumeException) params.get(CUR_EXCEPTION);
        }
      }
    }.run(path, null);

  }

  /**
   * Rename file (or directory)
   * 
   * @param src
   * @param dst
   * @throws VolumeException
   */
  public boolean rename(String src, String dst) throws VolumeException {

    Map<String, Object> params = new HashMap<String, Object>();
    Map<String, String> innerParams = new HashMap<String, String>();
    innerParams.put(VolumeFSFile.ParamKey.PATH.name().toLowerCase(), dst);
    params.put("params", innerParams);

    return new VolumeFSJobRunnerProxy<Boolean>() {

      @SuppressWarnings("unchecked")
      @Override
      public Boolean doJob(String path, Map<String, Object> params) throws VolumeException {

        VolumeFSFile file =
            odps.volumes().get(VolumeFSUtil.getVolumeFromPath(path)).getVolumeFSFile(path);
        file.update((Map<String, String>) params.get("params"));
        return true;
      }

      @Override
      public Boolean onVolumeMissing(String path, Map<String, Object> params)
          throws VolumeException {
        return false;
      }

      @Override
      public Boolean onNoSuchVolume(String path, Map<String, Object> params) throws VolumeException {
        throw new VolumeException(VolumeFSErrorCode.NoSuchPath,
            VolumeFSErrorMessageGenerator.noSuchFileOrDirectory(path));
      }

      @Override
      public Boolean onInvalidPath(String path, Map<String, Object> params) throws VolumeException {
        if (VolumeFSUtil.checkPathIsJustVolume(path)) {
          throw new VolumeException(VolumeFSErrorCode.NotAcceptableOperation,
              VolumeFSErrorMessageGenerator
                  .theOpreationIsNotAllowed("mv volume (The first level directory)"));
        } else {
          throw (VolumeException) params.get(CUR_EXCEPTION);
        }
      }
    }.run(src, params);

  }

  /**
   * Delete file (or directory)
   * 
   * @param path
   * @param recursive
   * @throws VolumeException
   */
  public boolean delete(String path, boolean recursive) throws VolumeException {

    Map<String, Object> params = new HashMap<String, Object>();
    params.put(VolumeFSFile.ParamKey.RECURSIVE.name().toLowerCase(), recursive);

    return new VolumeFSJobRunnerProxy<Boolean>() {

      @Override
      public Boolean doJob(String path, Map<String, Object> params) throws VolumeException {

        VolumeFSFile file =
            odps.volumes().get(VolumeFSUtil.getVolumeFromPath(path)).getVolumeFSFile(path);
        file.delete((Boolean) params.get(VolumeFSFile.ParamKey.RECURSIVE.name().toLowerCase()));
        return true;
      }

      @Override
      public Boolean onVolumeMissing(String path, Map<String, Object> params)
          throws VolumeException {
        return false;
      }

      @Override
      public Boolean onNoSuchVolume(String path, Map<String, Object> params) throws VolumeException {
        throw new VolumeException(VolumeFSErrorCode.NoSuchPath,
            VolumeFSErrorMessageGenerator.noSuchFileOrDirectory(path));
      }

      @Override
      public Boolean onInvalidPath(String path, Map<String, Object> params) throws VolumeException {
        // If the path is a volume name , will call volume API
        if (VolumeFSUtil.checkPathIsJustVolume(path)) {
          try {
            Volume v = odps.volumes().get(VolumeFSUtil.getVolumeFromPath(path));
            if (isNewVolume(v)) {
              odps.volumes().delete(VolumeFSUtil.getVolumeFromPath(path));
              return true;
            } else {
              throw new VolumeException(
                  VolumeFSErrorCode.NoSuchPath,
                  VolumeFSErrorMessageGenerator.oldVolumeAlert(VolumeFSUtil.getVolumeFromPath(path)));
            }
          } catch (OdpsException e) {
            throw new VolumeException(e);
          }
        } else {
          throw (VolumeException) params.get(CUR_EXCEPTION);
        }
      }

    }.run(path, params);

  }

  /**
   * Download volume file
   * 
   * @param path
   * @param start start of range
   * @param end start of range
   * @param targetFile
   * @param append
   * @throws IOException
   */
  public void downloadFile(String path, Long start, Long end, File targetFile, boolean append)
      throws VolumeException {
    VolumeFSUtil.checkPath(path);
    OutputStream outputStream = null;
    try {
      outputStream = new FileOutputStream(targetFile, append);
    } catch (FileNotFoundException e) {
      throw new VolumeException(e);
    }
    InputStream inputStream = null;
    try {
      inputStream = openInputStream(path, start, end);
      IOUtils.copy(inputStream, outputStream);
    } catch (IOException e) {
      throw new VolumeException(e);
    } finally {
      IOUtils.closeQuietly(outputStream);
      IOUtils.closeQuietly(inputStream);
    }
  }

  /**
   * Create an {@link InputStream} at the indicated Path
   * 
   * @param path
   * @param start
   * @param end
   * @throws IOException
   */
  public InputStream openInputStream(String path, Long start, Long end) throws VolumeException {
    VolumeFSUtil.checkPath(path);
    boolean compress =
        conf.getBoolean(VolumeFileSystemConfigKeys.ODPS_VOLUME_TRANSFER_COMPRESS_ENABLED, false);
    CompressOption compressOption = null;
    if (compress) {
      CompressOption.CompressAlgorithm a =
          CompressOption.CompressAlgorithm.valueOf(conf.get(
              VolumeFileSystemConfigKeys.ODPS_VOLUME_TRANSFER_COMPRESS_ALGORITHM,
              VolumeFSConstants.ODPS_ZLIB));
      compressOption = new CompressOption(a, 1, 0);
    }
    InputStream in = null;
    try {
      in =
          getVolumeTunnel().openInputStream(odps.getDefaultProject(), path, start, end,
              compressOption);
    } catch (TunnelException e) {
      throw new VolumeException(e);
    }
    return in;
  }

  /**
   * Create an {@link OutputStream} at the indicated Path
   * 
   * @param path
   * @param overwrite
   * @param replication
   * @throws IOException
   */
  public OutputStream openOutputStream(String path, boolean overwrite, short replication)
      throws VolumeException {
    VolumeFSUtil.checkPath(path);
    VolumeFSFile file = null;
    try {
      if (!odps.volumes().exists(VolumeFSUtil.getVolumeFromPath(path))) {
        odps.volumes().create(VolumeFSUtil.getVolumeFromPath(path),
            VolumeFSConstants.CREATE_VOLUME_COMMENT, Volume.Type.NEW);
      }
    } catch (OdpsException oe) {
      throw new VolumeException(oe);
    }
    try {
      file = getFileInfo(path);
    } catch (VolumeException e) {
      if (!VolumeFSErrorCode.NoSuchPath.equalsIgnoreCase(e.getErrCode())) {
        throw e;
      }
    }
    if (file != null) {
      if (overwrite) {
        if (file.getIsdir()) {
          throw new VolumeException(VolumeFSErrorCode.PathAlreadyExists,
              VolumeFSErrorMessageGenerator.fileExists(path));
        } else {
          delete(path, true);
        }
      } else {
        throw new VolumeException(VolumeFSErrorCode.PathAlreadyExists,
            VolumeFSErrorMessageGenerator.fileExists(path));
      }
    }
    VolumeFSTunnel tunnel = getVolumeTunnel();
    // create a zero-byte file to satisfy visibility before wrote data into file
    createZeroByteFile(path, replication, tunnel);
    boolean compress =
        conf.getBoolean(VolumeFileSystemConfigKeys.ODPS_VOLUME_TRANSFER_COMPRESS_ENABLED, false);
    CompressOption compressOption = null;
    if (compress) {
      CompressOption.CompressAlgorithm a =
          CompressOption.CompressAlgorithm.valueOf(conf.get(
              VolumeFileSystemConfigKeys.ODPS_VOLUME_TRANSFER_COMPRESS_ALGORITHM,
              VolumeFSConstants.ODPS_ZLIB));
      compressOption = new CompressOption(a, 1, 0);
    }
    OutputStream out = null;
    try {
      out =
          tunnel.openOutputStream(odps.getDefaultProject(), path, new Integer(replication),
              compressOption);
    } catch (TunnelException e) {
      throw new VolumeException(e);
    }
    return out;
  }

  /**
   * Create a zero-byte file
   * 
   * @param path
   * @param replication
   * @param tunnel
   * @throws VolumeException
   */
  public void createZeroByteFile(String path, short replication, VolumeFSTunnel tunnel)
      throws VolumeException {
    VolumeFSUtil.checkPath(path);
    OutputStream o = null;
    try {
      o =
          tunnel.openOutputStream(odps.getDefaultProject(), path, new Integer(replication),
              new CompressOption());
      String sessionId = VolumeFSTunnel.getUploadSessionId((VolumeOutputStream) o);
      tunnel.commit(odps.getDefaultProject(), path, sessionId);
    } catch (TunnelException e) {
      throw new VolumeException(e);
    }
  }

  /**
   * Commit the uploadSession
   * 
   * @param path
   * @param out
   * @param overwrite
   * @throws VolumeException
   */
  public void commitUploadSession(String path, VolumeOutputStream out, boolean overwrite)
      throws VolumeException {
    VolumeFSUtil.checkPath(path);
    try {
      out.close();
      String sessionId = VolumeFSTunnel.getUploadSessionId(out);
      VolumeFSFile file = null;
      try {
        file = getFileInfo(path);
      } catch (Exception e) {
      }
      if (file != null) {
        if (overwrite) {
          if (file.getIsdir()) {
            throw new VolumeException(VolumeFSErrorCode.PathAlreadyExists,
                VolumeFSErrorMessageGenerator.fileExists(path));
          } else {
            file.delete(true);
          }
        } else {
          if (file.getLength().longValue() == 0) {
            // Delete zero-byte file
            file.delete(true);
          } else {
            throw new VolumeException(VolumeFSErrorCode.PathAlreadyExists,
                VolumeFSErrorMessageGenerator.fileExists(path));
          }
        }
      }
      getVolumeTunnel().commit(odps.getDefaultProject(), path, sessionId);
    } catch (Exception e) {
      throw new VolumeException(e);
    }
  }

  private VolumeFSTunnel getVolumeTunnel() {
    VolumeFSTunnel tunnelFS = new VolumeFSTunnel(odps.clone());
    if (tunnelEndpoint != null)
      tunnelFS.setEndpoint(tunnelEndpoint);
    return tunnelFS;
  }

  private boolean isNewVolume(Volume v) {
    return Volume.Type.NEW.equals(v.getType());
  }
}
