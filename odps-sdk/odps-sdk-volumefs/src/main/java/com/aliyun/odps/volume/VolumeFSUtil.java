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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.StringUtils;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ReloadException;
import com.aliyun.odps.VolumeException;
import com.aliyun.odps.VolumeFSFile;
import com.aliyun.odps.Volumes;
import com.aliyun.odps.fs.VolumeFileSystemConfigKeys;
import com.aliyun.odps.tunnel.VolumeFSErrorCode;
import com.aliyun.odps.volume.protocol.VolumeFSConstants;

/**
 * Utility that wraps some Volume related utility methods
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFSUtil {

  /**
   * Whether the pathname is valid. Currently prohibits relative paths, names which contain a ":" or
   * "//", or other non-canonical paths.
   */
  public static boolean isValidName(String src) {
    if (src == null)
      return false;

    // Path must be absolute.
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }

    // Check for "~" ".." "." ":" "/"
    String[] components = StringUtils.split(src, '/');
    for (int i = 0; i < components.length; i++) {
      String element = components[i];
      if (element.equals("~") || element.equals(".") || element.equals("..")
          || (element.indexOf(":") >= 0) || (element.indexOf("/") >= 0)
          || (element.indexOf("\\") >= 0) || (element.indexOf("\0") >= 0)) {
        return false;
      }
      // The string may start or end with a /, but not have
      // "//" in the middle.
      if (element.isEmpty() && i != components.length - 1 && i != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get volume name from a specific {@link Path}
   * 
   * @param path
   * @throws VolumeException
   */
  public static String getVolumeFromPath(Path path) throws VolumeException {
    path = Path.getPathWithoutSchemeAndAuthority(path);
    if (path.depth() == 0) {
      throw new VolumeException(VolumeFSErrorCode.VolumeMissing, "No volume found!");
    } else {
      String p = path.toUri().getPath();
      String volume = p.split(VolumeFSConstants.SEPARATOR)[1];
      return volume;
    }
  }

  /**
   * Get volume name from a specific path str
   * 
   * @param pathStr
   * @throws VolumeException
   */
  public static String getVolumeFromPath(String pathStr) throws VolumeException {
    Path path = new Path(pathStr);
    return getVolumeFromPath(path);
  }


  public static String getExternalLocation(VolumeFSFile file) {
    if (file.getProperties() != null) {
      return
          file.getProperties().getOrDefault(Volumes.EXTERNAL_VOLUME_LOCATION_KEY, null);
    }
    return null;
  }

  /**
   * Transfer {@link VolumeFSFile} to {@link FileStatus}
   * 
   * @param file
   */
  public static FileStatus transferFile(VolumeFSFile file) {
    if (file == null) {
      return null;
    }

    String symlinkPath;
    try {
      symlinkPath = org.apache.commons.lang.StringUtils.isBlank(file.getSymlink()) ? getExternalLocation(file) : file.getSymlink();
    } catch (ReloadException e) {
      OdpsException exception = (OdpsException) e.getCause();
      System.err.println(file.getPath() + ": ls error: RequestId=" + exception.getRequestId() + ", ErrorCode="
                         + exception.getErrorCode() + ", ErrorMessage=" + exception.getMessage());
      return null;
    }

    Path symlink =
        (symlinkPath == null) ? null : new Path(symlinkPath);


    FileStatus fileStatus =
        new FileStatus(file.getLength(), file.getIsdir(),
                       Objects.isNull(file.getBlockReplications()) ? 1
                                                                   : file.getBlockReplications(),
                       Objects.isNull(file.getBlockSize())
                       ? VolumeFSConstants.DEFAULT_VOLUME_BLOCK_SIZE : file.getBlockSize(),
                       Objects.isNull(file.getModificationTime()) ? 0 : file.getModificationTime().getTime(),
                       Objects.isNull(file.getAccessTime()) ? 0 : file.getAccessTime().getTime(),
                       com.aliyun.odps.utils.StringUtils.isNullOrEmpty(file.getPermission()) ? null
                                                                                             :
                       new FsPermission(Short.valueOf(file.getPermission(), 8)),
                       file.getOwner(), file.getGroup(), symlink, new Path(
            VolumeFileSystemConfigKeys.VOLUME_URI_SCHEME + "://" + file.getProject(),
            file.getPath()));
    return fileStatus;
  }

  /**
   * Check if a path is just a volume
   * 
   * @param path
   */
  public static boolean checkPathIsJustVolume(String path) {
    if (org.apache.commons.lang.StringUtils.isBlank(path)) {
      return false;
    }
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    if (path.indexOf("/") == -1) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check if a path is root
   * 
   * @param path
   */
  public static boolean checkPathIsRoot(String path) {
    if (org.apache.commons.lang.StringUtils.isBlank(path)) {
      return false;
    }
    path = path.replaceAll("//", "/");
    return path.trim().equals(VolumeFSConstants.ROOT_PATH);
  }

  /**
   * Batch transfer {@link VolumeFSFile} to {@link FileStatus}
   * 
   * @param files s
   */
  public static FileStatus[] transferFiles(VolumeFSFile[] files) {
    if (files == null)
      return null;
    List<FileStatus> fileStatusList = new ArrayList<>();
    for (VolumeFSFile file : files) {
      FileStatus fileStatus = transferFile(file);
      if (fileStatus != null) {
        fileStatusList.add(fileStatus);
      }
    }

    return fileStatusList.toArray(new FileStatus[0]);
  }

  /**
   * Probe for a path being a parent of another
   * 
   * @param parent parent path
   * @param child possible child path
   * @return true if the parent's path matches the start of the child's
   */
  public static boolean isParentOf(Path parent, Path child) {
    URI parentURI = parent.toUri();
    String parentPath = parentURI.getPath();
    if (!parentPath.endsWith("/")) {
      parentPath += "/";
    }
    URI childURI = child.toUri();
    String childPath = childURI.getPath();
    return childPath.startsWith(parentPath);
  }

  public static void checkPath(String path) throws VolumeException {
    if (checkPathIsRoot(path)) {
      throw new VolumeException(VolumeFSErrorCode.VolumeMissing,
          "Root path is not supported this operation!");
    }
    if (checkPathIsJustVolume(path)) {
      throw new VolumeException(VolumeFSErrorCode.InvalidPath, "The path is just a volume!");
    }
    if (!isValidName(path)) {
      throw new VolumeException(VolumeFSErrorCode.InvalidPath,
          "The path contains illegal characters!");
    }
  }

}
