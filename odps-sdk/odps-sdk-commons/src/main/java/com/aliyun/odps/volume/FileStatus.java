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
 * Interface that represents the client side information for a file.
 */
public class FileStatus implements Comparable {

  private Path path;
  private long length;
  private boolean isdir;
  private long modification_time;

  public FileStatus() {
    this(0, false, 0, null);
  }

  public FileStatus(long length, boolean isdir,
                    long modification_time,
                    Path path) {
    this.length = length;
    this.isdir = isdir;
    this.modification_time = modification_time;
    this.path = path;
  }

  /**
   * Get the length of this file, in bytes.
   *
   * @return the length of this file, in bytes.
   */
  public long getLen() {
    return length;
  }

  /**
   * Is this a file?
   *
   * @return true if this is a file
   */
  public boolean isFile() {
    return !isdir;
  }

  /**
   * Is this a directory?
   *
   * @return true if this is a directory
   */
  public boolean isDirectory() {
    return isdir;
  }

  /**
   * Get the modification time of the file.
   *
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  public long getModificationTime() {
    return modification_time;
  }

  public Path getPath() {
    return path;
  }

  public void setPath(final Path p) {
    path = p;
  }

  /**
   * Compare this object to another object
   *
   * @param o
   *     the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object
   * is less than, equal to, or greater than the specified object.
   * @throws ClassCastException
   *     if the specified object's is not of
   *     type FileStatus
   */
  @Override
  public int compareTo(Object o) {
    FileStatus other = (FileStatus) o;
    return this.getPath().compareTo(other.getPath());
  }

  /**
   * Compare if this object is equal to another object
   *
   * @param o
   *     the object to be compared.
   * @return true if two file status has the same path name; false if not.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileStatus)) {
      return false;
    }
    FileStatus other = (FileStatus) o;
    return this.getPath().equals(other.getPath());
  }

  /**
   * Returns a hash code value for the object, which is defined as
   * the hash code of the path name.
   *
   * @return a hash code value for the path name.
   */
  @Override
  public int hashCode() {
    return getPath().hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("path=" + path);
    sb.append("; isDirectory=" + isdir);
    if (!isDirectory()) {
      sb.append("; length=" + length);
    }
    sb.append("; modification_time=" + modification_time);
    sb.append("}");
    return sb.toString();
  }
}
