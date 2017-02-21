package com.aliyun.odps.exec;

import com.aliyun.odps.NotImplementedException;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.exec.InputSplit;
import com.aliyun.odps.exec.Reporter;
import com.aliyun.odps.udf.ExecutionContext;

/**
 * Reporter implemented by ExecutionContext
 */
public class ExecutionContextReporter implements Reporter {

  private ExecutionContext ctx;

  public ExecutionContextReporter(ExecutionContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void setStatus(String status) {
    throw new NotImplementedException();
  }

  @Override
  public Counter getCounter(Enum<?> name) {
    return ctx.getCounter(name);
  }

  @Override
  public Counter getCounter(String group, String name) {
    return ctx.getCounter(group, name);
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    Counter cnt = ctx.getCounter(key);
    cnt.increment(amount);
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    Counter cnt = ctx.getCounter(group, counter);
    cnt.increment(amount);
  }

  @Override
  public InputSplit getInputSplit() throws UnsupportedOperationException {
    throw new NotImplementedException();
  }

  @Override
  public float getProgress() {
    throw new NotImplementedException();
  }

  @Override
  public void progress() {
    ctx.claimAlive();
  }

}
