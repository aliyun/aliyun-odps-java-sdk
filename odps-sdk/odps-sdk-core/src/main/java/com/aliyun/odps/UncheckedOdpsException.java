package com.aliyun.odps;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class UncheckedOdpsException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  private final OdpsException cause;

  public UncheckedOdpsException(OdpsException cause) {
    super(cause);
    this.cause = cause;
  }

  @Override
  public OdpsException getCause() {
    return cause;
  }

  @Override
  public String getMessage() {
    return cause.getMessage() + ", requestId: " + cause.getRequestId();
  }
}
