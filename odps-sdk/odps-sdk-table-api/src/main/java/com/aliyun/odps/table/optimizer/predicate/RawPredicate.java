package com.aliyun.odps.table.optimizer.predicate;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class RawPredicate extends Predicate {

  private final String rawExpr;

  public RawPredicate(String rawExpr) {
    super(PredicateType.RAW);
    this.rawExpr = rawExpr;
  }

  public static RawPredicate of(String rawExpr) {
    return new RawPredicate(rawExpr);
  }

  @Override
  public String toString() {
    return rawExpr;
  }
}
