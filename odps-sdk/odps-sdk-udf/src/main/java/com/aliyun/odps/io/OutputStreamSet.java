package com.aliyun.odps.io;

/**
 * Encapsulation class that hosts a collection of output streams, note that currently we only expose one output
 * stream to user, so effectively the set size is always 1, the class is here for future extension
 **/
public interface OutputStreamSet{
  /**
   * Access method for getting next output stream. Note that since only one single output stream is allowed in
   * OutputStreamSet, this interface will only return the single stream on the first invocation. Attempt to invoke
   * a second next call will trigger exception.
   * @return: SinkOutputStream.
   **/
  SinkOutputStream next();
}
