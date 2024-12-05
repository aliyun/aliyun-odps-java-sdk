package com.aliyun.odps.table.optimizer.predicate;

import java.util.Objects;

import com.aliyun.odps.table.utils.Preconditions;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Attribute extends Predicate {

  private final String value;
  private static final String BACK_TICK = "`";
  private static final String ESCAPE_BACK_TICK = "``";

  public Attribute(String value) {
    super(PredicateType.ATTRIBUTE);
    Preconditions.checkNotNull(value, "Attribute value");
    this.value = value;
  }

  public static Attribute of(String value) {
    return new Attribute(value);
  }

  @Override
  public String toString() {
    if (value.startsWith(BACK_TICK) && value.endsWith(BACK_TICK) && value.length() > 1) {
      // Already properly quoted
      return value;
    } else {
      // Escape backticks and add quotes
      return BACK_TICK + value.replace(BACK_TICK, ESCAPE_BACK_TICK) + BACK_TICK;
    }
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
