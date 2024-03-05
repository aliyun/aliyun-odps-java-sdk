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
package com.aliyun.odps.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.VolumeException;
import com.aliyun.odps.VolumeFSFile;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.VolumeFSErrorCode;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.volume.VolumeFSClient;
import com.aliyun.odps.volume.VolumeFSInputStream;
import com.aliyun.odps.volume.VolumeFSOutputStream;
import com.aliyun.odps.volume.VolumeFSUtil;
import com.aliyun.odps.volume.protocol.VolumeFSConstants;
import com.aliyun.odps.volume.protocol.VolumeFSErrorMessageGenerator;

/**
 * The ODPS Volume implementation of Hadoop {@link FileSystem}
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFileSystem extends FileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(VolumeFileSystem.class);

  /*
   * Current project
   */
  private String project;

  private String homeVolume;

  private short defaultReplication;

  private Path workingDir;

  private URI uri;

  private VolumeFSClient volumeClient;


  public VolumeFileSystem() {}

  /**
   * Return the protocol scheme for the FileSystem.
   * <p/>
   *
   * @return <code>odps</code>
   */
  @Override
  public String getScheme() {
    return VolumeFileSystemConfigKeys.VOLUME_URI_SCHEME;
  }


  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    conf.addResource(VolumeFSConstants.VOLUME_FS_CONFIG_FILE);
    super.initialize(uri, conf);
    setConf(conf);
    checkURI(uri);
    this.project = resolveProject(uri);
    this.volumeClient = createVolumeClient(conf);
    this.uri =
        URI.create(uri.getScheme() + VolumeFSConstants.SCHEME_SEPARATOR + uri.getAuthority());
    this.homeVolume = getHomeVolume(conf);
    this.workingDir = getHomeDirectory();
    this.defaultReplication =
        (short) conf.getInt(VolumeFileSystemConfigKeys.DFS_REPLICATION_KEY,
            VolumeFSConstants.DFS_REPLICATION_DEFAULT);
  }

  /*
   * You should create the homeVolume manually first
   */
  private String getHomeVolume(Configuration conf) {
    String defaultVolume = conf.get(VolumeFileSystemConfigKeys.ODPS_HOME_VOLMUE);
    if (defaultVolume != null) {
      return defaultVolume;
    } else {
      return VolumeFSConstants.DEFAULT_HOME_VOLUME;
    }
  }

  private String resolveProject(URI uri) {
    return uri.getAuthority();
  }

  private void checkURI(URI uri) throws IOException {
    String authority = uri.getAuthority();
    if (authority == null) {
      throw new IOException("Incomplete ODPS URI, no project: " + uri);
    }
  }

  private VolumeFSClient createVolumeClient(Configuration conf) throws IOException {
    String accessId = conf.get(VolumeFileSystemConfigKeys.ODPS_ACCESS_ID);
    String accessKey = conf.get(VolumeFileSystemConfigKeys.ODPS_ACCESS_KEY);
    if (accessId == null || accessKey == null) {
      throw new IOException("Incomplete config, no accessId or accessKey");
    }
    String serviceEndpoint = conf.get(VolumeFileSystemConfigKeys.ODPS_SERVICE_ENDPOINT);
    String tunnelEndpoint = conf.get(VolumeFileSystemConfigKeys.ODPS_TUNNEL_ENDPOINT);
    if (serviceEndpoint == null) {
      throw new IOException("Incomplete config, no "
          + VolumeFileSystemConfigKeys.ODPS_SERVICE_ENDPOINT);
    }
    Account account = new AliyunAccount(accessId, accessKey);
    Odps odps = new Odps(account);

    String restClientRetryTime = conf.get(VolumeFileSystemConfigKeys.ODPS_RESTCLIENT_RETRYTIME);
    if (!StringUtils.isNullOrEmpty(restClientRetryTime)) {
      odps.getRestClient().setRetryTimes(Integer.parseInt(restClientRetryTime));
    }

    odps.setEndpoint(serviceEndpoint);
    return new VolumeFSClient(odps, project, serviceEndpoint, tunnelEndpoint, conf);
  }

  @Override
  public Path getHomeDirectory() {
    return new Path(VolumeFSConstants.ROOT_PATH + homeVolume, System.getProperty("user.name"));
  }


  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    Path absF = fixRelativePart(f);
    String filePath = getPathName(absF);
    if (!exists(absF)) {
      throw new FileNotFoundException(filePath);
    }
    FileStatus fileStatus = getFileStatus(f);
    if (fileStatus.isDirectory()) {
      throw new FileNotFoundException(VolumeFSErrorMessageGenerator.isADirectory(filePath));
    }
    return new FSDataInputStream(new VolumeFSInputStream(filePath, volumeClient,
        fileStatus.getLen(), getConf()));
  }


  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    Path absF = fixRelativePart(f);
    String filePath = getPathName(absF);
    if (!VolumeFSUtil.isValidName(filePath)) {
      throw new IllegalArgumentException(
          VolumeFSErrorMessageGenerator.isNotAValidODPSVolumeFSFilename(filePath));
    }
    if (VolumeFSUtil.checkPathIsJustVolume(filePath)) {
      throw new IOException(
          VolumeFSErrorMessageGenerator.theOpreationIsNotAllowed("Create file in the root path!"));
    }
    try {
      return new FSDataOutputStream(new VolumeFSOutputStream(filePath, volumeClient, permission,
          overwrite, replication, blockSize, progress), statistics);
    } catch (VolumeException e) {
      logException(e);
      throw wrapExceptions(filePath, e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new IOException("Not supported");
  }


  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    statistics.incrementWriteOps(1);
    Path absSrc = fixRelativePart(src);
    Path absDst = fixRelativePart(dst);
    if (!exists(absSrc)) {
      throw new FileNotFoundException("Source path " + src + " does not exist");
    }
    if (isDirectory(absDst)) {
      // destination is a directory: rename goes underneath it with the
      // source name
      absDst = new Path(absDst, absSrc.getName());
    }
    if (exists(absDst)) {
      throw new FileAlreadyExistsException("Destination path " + dst + " already exists");
    }
    if (absDst.getParent() != null && !exists(absDst.getParent())) {
      throw new FileNotFoundException(VolumeFSErrorMessageGenerator.noSuchFileOrDirectory(absDst
          .getParent().toString()));
    }

    if (VolumeFSUtil.isParentOf(absSrc, absDst)) {
      throw new IOException("Cannot rename " + absSrc + " under itself" + " : " + absDst);
    }
    String srcPath = getPathName(absSrc);
    String dstPath = getPathName(absDst);
    try {
      return volumeClient.rename(srcPath, dstPath);
    } catch (VolumeException e) {
      logException(e);
      throw wrapExceptions(srcPath, e);
    }
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    String filePath = getPathName(absF);
    if (VolumeFSConstants.ROOT_PATH.equalsIgnoreCase(filePath.replaceAll("//", "/").trim())
        && !recursive) {
      throw new IOException("Non recursive delete is not allowed on root path");
    }
    try {
      getFileStatus(absF);
    } catch (IOException e) {
      if (e instanceof FileNotFoundException) {
        return false;
      }
    }
    try {
      return volumeClient.delete(filePath, recursive);
    } catch (VolumeException e) {
      IOException ioe = wrapExceptions(filePath, e);
      if (ioe instanceof FileNotFoundException) {
        return false;
      } else {
        logException(e);
        throw ioe;
      }
    }
  }


  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(f);
    String filePath = getPathName(absF);
    VolumeFSFile[] files;
    try {
      files = volumeClient.getFileInfosByPath(filePath);
    } catch (VolumeException e) {
      logException(e);
      throw wrapExceptions(filePath, e);
    }
    return VolumeFSUtil.transferFiles(files);
  }


  @Override
  public void setWorkingDirectory(Path new_dir) {
    Path dir = fixRelativePart(new_dir);
    String result = dir.toUri().getPath();
    if (!VolumeFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Invalid Volume directory name " + result);
    }
    workingDir = dir;
  }


  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }


  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(f);
    String filePath = getPathName(absF);
    try {
      return volumeClient.mkdirs(filePath);
    } catch (VolumeException e) {
      logException(e);
      throw wrapExceptions(filePath, e);
    }
  }

  @Override
  public boolean setReplication(Path src, short replication) throws IOException {
    statistics.incrementWriteOps(1);
    Path absF = fixRelativePart(src);
    String filePath = getPathName(absF);
    try {
      return volumeClient.setReplication(filePath, replication);
    } catch (VolumeException e) {
      logException(e);
      throw wrapExceptions(filePath, e);
    }
  }

  @Override
  public short getDefaultReplication() {
    return defaultReplication;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    statistics.incrementReadOps(1);
    Path absF = fixRelativePart(f);
    String filePath = getPathName(absF);
    VolumeFSFile file;
    try {
      file = volumeClient.getFileInfo(filePath);
    } catch (VolumeException e) {
      logException(e);
      throw wrapExceptions(filePath, e);
    }
    return VolumeFSUtil.transferFile(file);
  }


  @Override
  public FsStatus getStatus(Path p) throws IOException {
    // TODO
    return new FsStatus(0, 0, 0);
  }

  private IOException wrapExceptions(String path, VolumeException e) {
    if (VolumeFSErrorCode.NoSuchPath.equalsIgnoreCase(e.getErrCode())) {
      return new FileNotFoundException(VolumeFSErrorMessageGenerator.noSuchFileOrDirectory(path));
    }
    if (VolumeFSErrorCode.InvalidPath.equalsIgnoreCase(e.getErrCode())) {
      throw new IllegalArgumentException(
          VolumeFSErrorMessageGenerator.isNotAValidODPSVolumeFSFilename(path));
    }
    if (VolumeFSErrorCode.NotAcceptableOperation.equalsIgnoreCase(e.getErrCode())) {
      throw new UnsupportedOperationException(e.getMessage());
    }
    if (VolumeFSErrorCode.PathAlreadyExists.equalsIgnoreCase(e.getErrCode())) {
      return new FileAlreadyExistsException(e.getMessage());
    }
    if (VolumeFSErrorCode.ParentNotDirectory.equalsIgnoreCase(e.getErrCode())) {
      return new ParentNotDirectoryException(e.getMessage());
    } else {
      return new IOException(e.getMessage(), e);
    }
  }

  /**
   * Checks that the passed URI belongs to this filesystem and returns just the path component.
   * Expects a URI with an absolute path.
   * 
   * @param file URI with absolute path
   * @return path component of {file}
   * @throws IllegalArgumentException if URI does not belong to this DFS
   */
  private String getPathName(Path file) {
    checkPath(file);
    String result = file.toUri().getPath();
    if (!VolumeFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Pathname " + result + " from " + file
          + " is not a valid VolumeFS filename.");
    }
    return result;
  }

  private void logException(VolumeException e) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(e.getMessage(), e);
    }
  }


}
