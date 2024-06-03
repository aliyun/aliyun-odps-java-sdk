package com.aliyun.odps.udf;

import java.util.Collection;

public interface Committer {
  void setup(ExecutionContext context, DataAttributes parameters);
  /**
   * @param commitMessage combined of all VectorizedOutputer committed commitMessage
   * @return true if commit success, false if commit failed
   */
  boolean commitTableWrite(Collection<CommitMessage> commitMessage);
}