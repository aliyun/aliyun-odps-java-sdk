package com.aliyun.odps.table.optimizer.predicate;

import java.io.Serializable;
import java.time.temporal.Temporal;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Constant extends Predicate {

  private final Serializable value;

  public Constant(Object value) {
    super(PredicateType.CONSTANT);
    this.value = (Serializable) value;
  }

  public static Constant of(Object value) {
    return new Constant(value);
  }

  private boolean isStringType() {
    return value instanceof String || value instanceof Character;
  }

  private boolean isTimeType() {
    return value instanceof java.util.Date || value instanceof Temporal;
  }


  @Override
  public String toString() {
    if (isStringType() || isTimeType()) {
      return "'" + value + "'";
    }
    return value.toString();
  }
}
