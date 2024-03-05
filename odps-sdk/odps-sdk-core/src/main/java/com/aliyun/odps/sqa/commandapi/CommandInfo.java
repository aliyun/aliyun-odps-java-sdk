package com.aliyun.odps.sqa.commandapi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.sqa.commandapi.Command;
import com.aliyun.odps.utils.StringUtils;

public class CommandInfo {

  private String commandText;
  private Command command;
  private Instance instance;
  private String taskName;
  private boolean odpsNamespaceSchema;
  private Map<String, String> hint;
  private List<String> executionLog = new ArrayList<>();

  public CommandInfo(String commandText, Map<String, String> hint) {
    this.commandText = commandText;
    this.hint = hint;
  }

  public String getCommandText() {
    return commandText;
  }

  public Command getCommand() {
    return command;
  }

  public void setCommand(Command commmand) {
    this.command = commmand;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  public boolean isOdpsNamespaceSchema() {
    return odpsNamespaceSchema;
  }

  public void setOdpsNamespaceSchema(boolean odpsNamespaceSchema) {
    this.odpsNamespaceSchema = odpsNamespaceSchema;
  }

  public Map<String, String> getHint() {
    return hint;
  }

  public void setHint(Map<String, String> hint) {
    this.hint = hint;
  }

  public Instance getInstance() {
    return instance;
  }

  public void setInstance(Instance instance, String logview, String rerunMessage) {
    synchronized (this) {
      this.instance = instance;
      if (!StringUtils.isNullOrEmpty(rerunMessage)) {
        // rerun or fallback
        executionLog.add("command failed:" + rerunMessage);
      }
      executionLog.add(logview);
    }
  }
}
