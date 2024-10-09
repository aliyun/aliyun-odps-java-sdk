package com.aliyun.odps.exceptions;

import java.io.IOException;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SchemaMismatchException extends IOException {

  private final String latestSchemaVersion;

  public SchemaMismatchException(String msg, String latestSchemaVersion) {
    super(msg);
    this.latestSchemaVersion = latestSchemaVersion;
  }

  public String getLatestSchemaVersion() {
    return latestSchemaVersion;
  }
}
