package com.aliyun.odps.sqa;

import com.aliyun.odps.Instance;
import com.aliyun.odps.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dongxiao on 2020/3/16.
 */
class QueryInfo {
  // SESSION subquery id
  private int id = -1;
  private int retry = -1;
  private boolean isSelect = true;
  private String sql;
  private Map<String, String> hint;
  // offline: SQLTASK instance
  // online: SQLRT instance
  private Instance instance = null;
  private ExecuteMode executeMode = ExecuteMode.INTERACTIVE;
  private List<String> executionLog = new ArrayList<>();

  QueryInfo(String sql, Map<String, String> hint, ExecuteMode executeMode) {
    this.sql = sql;
    this.hint = hint;
    this.executeMode = executeMode;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getRetry() {
    synchronized (this) {
      return retry;
    }
  }

  public void incRetry() {
    synchronized (this) {
      this.retry++;
    }
  }

  public String getSql() {
    return sql;
  }

  public Map<String, String> getHint() {
    return hint;
  }

  public void setHint(Map<String, String> hint) {
    this.hint = hint;
  }

  public Instance getInstance() {
    synchronized (this) {
      return instance;
    }
  }

  public ExecuteMode getExecuteMode() {
    synchronized (this) {
      return executeMode;
    }
  }

  public void setExecuteMode(ExecuteMode mode) {
    synchronized (this) {
      executeMode = mode;
    }
  }

  public void setInstance(Instance instance, ExecuteMode executeMode, String logview, String rerunMessage) {
    synchronized (this) {
      this.instance = instance;
      this.executeMode = executeMode;
      if (!StringUtils.isNullOrEmpty(rerunMessage)) {
        // rerun or fallback
        executionLog.add("Query failed:" + rerunMessage);
        if (executeMode.equals(ExecuteMode.OFFLINE)) {
          executionLog.add("Will fallback to offline mode");
        } else {
          executionLog.add("Will rerun in interactive mode");
        }
      }
      executionLog.add("Running in " + executeMode.toString().toLowerCase()
          + " mode, RetryCount: " + retry + ", QueryId:" + id + "\nLog view:");
      executionLog.add(logview);
    }
  }

  public boolean isSelect() {
    return isSelect;
  }

  public void setSelect(boolean select) {
    isSelect = select;
  }

  public List<String> getAndCleanExecutionLog() {
    synchronized (this) {
      List<String> log = new ArrayList<>();
      log.addAll(executionLog);
      executionLog.clear();
      return log;
    }
  }
}
