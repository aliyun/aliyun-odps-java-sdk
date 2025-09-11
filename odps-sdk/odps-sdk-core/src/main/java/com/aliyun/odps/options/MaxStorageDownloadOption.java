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

  // nano, micro, milli, second
  private String timestampUnit;
  private String dateTimeUnit;

  private MaxStorageDownloadOption(Builder builder) {
    this.requiredColumns = builder.requiredColumns;
    this.splitSize = builder.splitSize;
    this.timestampUnit = builder.timestampUnit;
    this.dateTimeUnit = builder.dateTimeUnit;
  }

  public List<String> getRequiredColumns() {
    return requiredColumns;
  }

  public Long getSplitSize() {
    return splitSize;
  }

  public String getTimestampUnit() {
    return timestampUnit;
  }

  public String getDateTimeUnit() {
    return dateTimeUnit;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for MaxStorageDownloadOption.
   */
  public static class Builder {

    private List<String> requiredColumns;
    private Long splitSize;
    // nano, micro, milli, second
    private String timestampUnit = "nano";
    private String dateTimeUnit = "milli";

    public Builder setRequiredColumns(List<String> requiredColumns) {
      this.requiredColumns = requiredColumns;
      return this;
    }

    public Builder setSplitSize(Long splitSize) {
      this.splitSize = splitSize;
      return this;
    }

    public Builder setDateTimeUnit(String dateTimeUnit) {
      this.dateTimeUnit = dateTimeUnit;
      return this;
    }

    public Builder setTimestampUnit(String timestampUnit) {
      this.timestampUnit = timestampUnit;
      return this;
    }

    public MaxStorageDownloadOption build() {
      // You can add validation logic here if needed
      return new MaxStorageDownloadOption(this);
    }
  }

}
