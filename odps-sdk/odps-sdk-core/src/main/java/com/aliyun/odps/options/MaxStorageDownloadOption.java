package com.aliyun.odps.options;

import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MaxStorageDownloadOption {

  private final List<String> requiredColumns;

  /**
   * default 256 MB, which is the compressed file size
   */
  private final Long splitSize;

  private MaxStorageDownloadOption(Builder builder) {
    this.requiredColumns = builder.requiredColumns;
    this.splitSize = builder.splitSize;
  }

  public List<String> getRequiredColumns() {
    return requiredColumns;
  }

  public Long getSplitSize() {
    return splitSize;
  }

  /**
   * Builder class for MaxStorageDownloadOption.
   */
  public static class Builder {

    private List<String> requiredColumns;
    private Long splitSize;

    public Builder setRequiredColumns(List<String> requiredColumns) {
      this.requiredColumns = requiredColumns;
      return this;
    }

    public Builder setSplitSize(Long splitSize) {
      this.splitSize = splitSize;
      return this;
    }

    public MaxStorageDownloadOption build() {
      // You can add validation logic here if needed
      return new MaxStorageDownloadOption(this);
    }
  }

}
