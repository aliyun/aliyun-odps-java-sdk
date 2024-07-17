package com.aliyun.odps.table.optimizer.predicate;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Attribute extends Predicate {

  private final Serializable value;

  public Attribute(Object value) {
    super(PredicateType.ATTRIBUTE);
    this.value = (Serializable) value;
  }

  public static Attribute of(Object value) {
    return new Attribute(value);
  }

  @Override
  public String toString() {
    return "`" + value + "`";
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
    Attribute attribute = (Attribute) o;
    return Objects.equals(value, attribute.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), value);
  }
}
