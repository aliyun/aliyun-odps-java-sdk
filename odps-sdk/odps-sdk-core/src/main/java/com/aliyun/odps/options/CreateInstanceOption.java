package com.aliyun.odps.options;

import com.aliyun.odps.sqa.v2.MaxQAConnInfo;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CreateInstanceOption {

  private String projectName;
  private Integer priority;
  private String runningCluster;
  private String jobName;
  private MaxQAConnInfo maxQAConnInfo;
  private boolean tryWait;
  private String uniqueIdentifyID;

  // Private constructor to ensure instances are created via Builder
  private CreateInstanceOption(Builder builder) {
    this.projectName = builder.projectName;
    this.priority = builder.priority;
    this.runningCluster = builder.runningCluster;
    this.jobName = builder.jobName;
    this.maxQAConnInfo = builder.maxQAConnInfo;
    this.tryWait = builder.tryWait;
    this.uniqueIdentifyID = builder.uniqueIdentifyID;
  }


  public String getProjectName() {
    return projectName;
  }

  public Integer getPriority() {
    return priority;
  }

  public String getRunningCluster() {
    return runningCluster;
  }

  public String getJobName() {
    return jobName;
  }

  public MaxQAConnInfo getMaxQAConnInfo() {
    return maxQAConnInfo;
  }

  public boolean isTryWait() {
    return tryWait;
  }

  public String getUniqueIdentifyID() {
    return uniqueIdentifyID;
  }

  // Builder static inner class
  public static class Builder {

    private String projectName;
    private Integer priority;
    private String runningCluster;
    private String jobName;
    private MaxQAConnInfo maxQAConnInfo;
    private boolean tryWait;
    private String uniqueIdentifyID;

    public Builder setProjectName(String projectName) {
      this.projectName = projectName;
      return this;
    }

    public Builder setPriority(Integer priority) {
      this.priority = priority;
      return this;
    }

    public Builder setRunningCluster(String runningCluster) {
      this.runningCluster = runningCluster;
      return this;
    }

    public Builder setJobName(String jobName) {
      this.jobName = jobName;
      return this;
    }

    public Builder setMaxQAConnInfo(MaxQAConnInfo maxQAConnInfo) {
      this.maxQAConnInfo = maxQAConnInfo;
      return this;
    }

    public Builder setTryWait(boolean tryWait) {
      this.tryWait = tryWait;
      return this;
    }

    public Builder setUniqueIdentifyID(String uniqueIdentifyID) {
      this.uniqueIdentifyID = uniqueIdentifyID;
      return this;
    }

    // Build method to create an instance of CreateInstanceOption
    public CreateInstanceOption build() {
      return new CreateInstanceOption(this);
    }
  }
}
