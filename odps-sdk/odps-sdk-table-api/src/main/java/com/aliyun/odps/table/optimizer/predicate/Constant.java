package com.aliyun.odps.table.optimizer.predicate;

import java.io.Serializable;
import java.time.temporal.Temporal;
import java.util.Objects;

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


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    Constant constant = (Constant) o;
    return Objects.equals(value, constant.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), value);
  }
}
