package com.aliyun.odps.io;

/**
 * Encapsulation class that hosts a collection of input streams, each corresponding
 * to a file.  This exposes limited access method for getting a SourceInputStream
 * at a time from the collection.
 **/
public interface InputStreamSet {

  /**
   * Access method for getting next available stream
   * @return: SourceInputStream, or null when there is no more stream.
   **/
  SourceInputStream next();
}
