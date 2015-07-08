/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.volume;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

/**
 * VolumeFileSystem have volume partition dir as root, so all paths are
 * relative to partition dir.
 * <p>Only available at job runtime.
 */
public abstract class FileSystem {

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param f
   *     the file name to open
   * @param bufferSize
   *     the size of the buffer to be used.
   */
  public abstract FSDataInputStream open(Path f, int bufferSize)
      throws IOException;

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param f
   *     the file to open
   */
  public abstract FSDataInputStream open(Path f) throws IOException;

  /**
   * Create an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   *
   * @param f
   *     the file to create
   */
  public FSDataOutputStream create(Path f) throws IOException {
    return create(f, true);
  }

  /**
   * Create an FSDataOutputStream at the indicated Path.
   *
   * @param f
   *     the file to create
   * @param overwrite
   *     if a file with this name already exists, then if true,
   *     the file will be overwritten, and if false an exception will be thrown.
   */
  public abstract FSDataOutputStream create(Path f, boolean overwrite)
      throws IOException;

  /**
   * Create an FSDataOutputStream at the indicated Path.
   *
   * @param f
   *     the file name to create
   * @param overwrite
   *     if a file with this name already exists, then if true,
   *     the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize
   *     the size of the buffer to be used.
   */
  public abstract FSDataOutputStream create(Path f,
                                            boolean overwrite,
                                            int bufferSize
  ) throws IOException;

  /**
   * Creates the given Path as a brand-new zero-length file.  If
   * create fails, or if it already existed, return false.
   *
   * @param f
   *     path to use for create
   */
  public boolean createNewFile(Path f) throws IOException {
    if (exists(f)) {
      return false;
    } else {
      create(f, false).close();
      return true;
    }
  }

  /**
   * Append to an existing file (optional operation).
   *
   * @param f
   *     the existing file to be appended.
   * @throws IOException
   */
  public abstract FSDataOutputStream append(Path f) throws IOException;

  /**
   * Append to an existing file (optional operation).
   * Same as append(f, bufferSize, null).
   *
   * @param f
   *     the existing file to be appended.
   * @param bufferSize
   *     the size of the buffer to be used.
   * @throws IOException
   */
  public abstract FSDataOutputStream append(Path f, int bufferSize) throws IOException;

  /**
   * Renames Path src to Path dst.  Can take place on local fs
   * or remote DFS.
   *
   * @param src
   *     path to be renamed
   * @param dst
   *     new path after rename
   * @return true if rename is successful
   * @throws IOException
   *     on failure
   */
  public abstract boolean rename(Path src, Path dst) throws IOException;


  /**
   * Delete a file.
   * Always recursive delete.
   *
   * @param f
   *     the path to delete.
   * @return true if delete is successful else false.
   * @throws IOException
   */
  public abstract boolean delete(Path f) throws IOException;

  /**
   * Non-recursive delete not supported, just for compatible with hadoop.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    if (!recursive) {
      throw new RuntimeException("Non-recursive delete not implemented...");
    } else {
      return delete(f);
    }
  }

  /**
   * Check if exists.
   *
   * @param f
   *     source file
   */
  public boolean exists(Path f) throws IOException {
    try {
      return getFileStatus(f) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * True iff the named path is a directory.
   * Note: Avoid using this method. Instead reuse the FileStatus
   * returned by getFileStatus() or listStatus() methods.
   *
   * @param f
   *     path to check
   */
  public boolean isDirectory(Path f) throws IOException {
    try {
      return getFileStatus(f).isDirectory();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /**
   * True iff the named path is a regular file.
   * Note: Avoid using this method. Instead reuse the FileStatus
   * returned by getFileStatus() or listStatus() methods.
   *
   * @param f
   *     path to check
   */
  public boolean isFile(Path f) throws IOException {
    try {
      return getFileStatus(f).isFile();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path file) {
      return true;
    }
  };

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   *
   * @param f
   *     given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException
   *     when the path does not exist;
   *     IOException see specific implementation
   */
  public abstract FileStatus[] listStatus(Path f) throws FileNotFoundException,
                                                         IOException;

  /*
   * Filter files/directories in the given path using the user-supplied path
   * filter. Results are added to the given array <code>results</code>.
   */
  private void listStatus(ArrayList<FileStatus> results, Path f,
                          PathFilter filter) throws FileNotFoundException, IOException {
    FileStatus listing[] = listStatus(f);
    if (listing == null) {
      throw new IOException("Error accessing " + f);
    }

    for (int i = 0; i < listing.length; i++) {
      if (filter.accept(listing[i].getPath())) {
        results.add(listing[i]);
      }
    }
  }

  /**
   * Filter files/directories in the given path using the user-supplied path
   * filter.
   *
   * @param f
   *     a path name
   * @param filter
   *     the user-supplied path filter
   * @return an array of FileStatus objects for the files under the given path
   * after applying the filter
   * @throws FileNotFoundException
   *     when the path does not exist;
   *     IOException see specific implementation
   */
  public FileStatus[] listStatus(Path f, PathFilter filter)
      throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    listStatus(results, f, filter);
    return results.toArray(new FileStatus[results.size()]);
  }

  /**
   * Filter files/directories in the given list of paths using default
   * path filter.
   *
   * @param files
   *     a list of paths
   * @return a list of statuses for the files under the given paths after
   * applying the filter default Path filter
   * @throws FileNotFoundException
   *     when the path does not exist;
   *     IOException see specific implementation
   */
  public FileStatus[] listStatus(Path[] files)
      throws FileNotFoundException, IOException {
    return listStatus(files, DEFAULT_FILTER);
  }

  /**
   * Filter files/directories in the given list of paths using user-supplied
   * path filter.
   *
   * @param files
   *     a list of paths
   * @param filter
   *     the user-supplied path filter
   * @return a list of statuses for the files under the given paths after
   * applying the filter
   * @throws FileNotFoundException
   *     when the path does not exist;
   *     IOException see specific implementation
   */
  public FileStatus[] listStatus(Path[] files, PathFilter filter)
      throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    for (int i = 0; i < files.length; i++) {
      listStatus(results, files[i], filter);
    }
    return results.toArray(new FileStatus[results.size()]);
  }

  /**
   * Make the given file and all non-existent parents into
   * directories. Has the semantics of Unix 'mkdir -p'.
   * Existence of the directory hierarchy is not an error.
   *
   * @param f
   *     path to create
   */
  public abstract boolean mkdirs(Path f) throws IOException;

  /**
   * Return a file status object that represents the path.
   *
   * @param f
   *     The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException
   *     when the path does not exist;
   *     IOException see specific implementation
   */
  public abstract FileStatus getFileStatus(Path f) throws IOException;

}
