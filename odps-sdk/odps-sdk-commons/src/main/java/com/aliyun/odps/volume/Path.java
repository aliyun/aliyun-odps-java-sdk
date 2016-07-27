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

/**
 * Volume分区下文件和目录的路径.
 * 仅用于表示volume partition下的文件和目录, 根目录表示相应的volume partition目录.
 */
public class Path implements Comparable {

  /**
   * The directory separator, a slash.
   */
  public static final String SEPARATOR = "/";

  private static final String CUR_DIR = "/";

  /**
   * this is always an absolute path
   */
  private String path;

  /**
   * Resolve a child path against a parent path.
   */
  public Path(String parent, String child) {
    this(new Path(parent), new Path(child));
  }

  /**
   * Resolve a child path against a parent path.
   */
  public Path(Path parent, String child) {
    this(parent, new Path(child));
  }

  /**
   * Resolve a child path against a parent path.
   */
  public Path(String parent, Path child) {
    this(new Path(parent), child);
  }

  /**
   * Resolve a child path against a parent path.
   */
  public Path(Path parent, Path child) {
    // XXX child may be absolute just as hadoop Path & java.net.URI
    if (child.isAbsolute()) {
      path = child.path;
    }

    StringBuilder sb = new StringBuilder(parent.path.length() + 1 + child.path.length());
    sb.append(parent.path);
    if (!parent.path.equals("/")) {
      sb.append("/");
    }
    sb.append(child);
    path = sb.toString();
  }

  private static boolean isAbsolutePath(String path) {
    return path.startsWith(SEPARATOR);
  }

  private void checkPathArg(String path) throws IllegalArgumentException {
    // disallow construction of a Path from an empty string
    if (path == null) {
      throw new IllegalArgumentException(
          "Can not create a Path from a null string");
    }
    if (path.length() == 0) {
      throw new IllegalArgumentException(
          "Can not create a Path from an empty string");
    }
  }

  /**
   * Construct a path from a String.  Path strings are URIs, but with
   * unescaped elements and some additional normalization.
   */
  public Path(String pathString) throws IllegalArgumentException {
    checkPathArg(pathString);

    path = normalizePath(pathString);
  }

  /**
   * Normalize a path string to use non-duplicated forward slashes as
   * the path separator and remove any trailing path separators.
   *
   * @return Normalized path string.
   */
  private static String normalizePath(String path) {
    // Remove double forward slashes.
    path = path.replaceAll("//", "/");

    // trim trailing slash from non-root path
    int minLength = 1;
    if (path.length() > minLength && path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  /**
   * Return the number of elements in this path.
   */
  public int depth() {
    int depth = 0;
    int slash = path.length() == 1 && path.charAt(0) == '/' ? -1 : 0;
    while (slash != -1) {
      depth++;
      slash = path.indexOf(SEPARATOR, slash + 1);
    }
    return depth;
  }

  public boolean isAbsolute() {
    return isAbsolutePath(path);
  }

  /**
   * @return true if and only if this path represents the root of a file system
   */
  public boolean isRoot() {
    return getParent() == null;
  }

  /**
   * Returns the final component of this path.
   */
  public String getName() {
    int slash = path.lastIndexOf(SEPARATOR);
    return path.substring(slash + 1);
  }

  /**
   * Returns the parent of a path or null if at root.
   */
  public Path getParent() {
    int lastSlash = path.lastIndexOf('/');
    int start = 0;
    if ((path.length() == start) ||               // empty path
        (lastSlash == start && path.length() == start + 1)) { // at root
      return null;
    }
    String parent;
    if (lastSlash == -1) {
      parent = CUR_DIR;
    } else {
      int end = 0;
      parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
    }
    return new Path(parent);
  }

  /**
   * Adds a suffix to the final name in the path.
   */
  public Path suffix(String suffix) {
    return new Path(getParent(), getName() + suffix);
  }

  @Override
  public String toString() {
    return path;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Path)) {
      return false;
    }
    Path that = (Path) o;
    return this.path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }

  @Override
  public int compareTo(Object o) {
    Path that = (Path) o;
    return this.path.compareTo(that.path);
  }
}
