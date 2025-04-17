package com.aliyun.odps.options;

import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SQLTaskOption {

  private static final String DEFAULT_TASK_NAME = "AnonymousSQLTask";
  private static final String DEFAULT_TASK_TYPE = "sql";

  CreateInstanceOption instanceOption;
  String taskName;
  Map<String, String> hints;
  Map<String, String> aliases;
  String type;

  public CreateInstanceOption getInstanceOption() {
    return instanceOption;
  }

  public String getTaskName() {
    return taskName;
  }

  public Map<String, String> getHints() {
    return hints;
  }

  public Map<String, String> getAliases() {
    return aliases;
  }

  public String getType() {
    return type;
  }

  private SQLTaskOption(Builder builder) {
    this.instanceOption = builder.instanceOption;
    this.taskName = builder.taskName;
    this.hints = builder.hints;
    this.aliases = builder.aliases;
    this.type = builder.type;
  }


  public static class Builder {

    private CreateInstanceOption instanceOption;
    private String taskName = DEFAULT_TASK_NAME;
    private Map<String, String> hints;
    private Map<String, String> aliases;
    private String type = DEFAULT_TASK_TYPE;


    public Builder setInstanceOption(CreateInstanceOption instanceOption) {
      this.instanceOption = instanceOption;
      return this;
    }

    public Builder setTaskName(String taskName) {
      this.taskName = taskName;
      return this;
    }

    public Builder setHints(Map<String, String> hints) {
      this.hints = hints;
      return this;
    }

    public Builder setAliases(Map<String, String> aliases) {
      this.aliases = aliases;
      return this;
    }

    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    // Build method to create an instance of SQLTaskOption
    public SQLTaskOption build() {
      return new SQLTaskOption(this);
    }
  }
}
