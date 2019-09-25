package com.aliyun.odps.data;

public abstract class  AbstractChar<T extends AbstractChar> {
  protected String value;

  AbstractChar(String value) {
    if (value == null) {
      this.value = null;
    } else {
      this.value = enforceMaxLength(value, value.length());
    }
  }

  AbstractChar(String value, int maxLength) {
    this.value = enforceMaxLength(value, maxLength);
  }

  private  String enforceMaxLength(String val, int maxLength) {
    if (val == null) {
      return null;
    }
    String value = val;

    if (maxLength > 0) {
      int valLength = val.codePointCount(0, val.length());
      if (valLength > maxLength) {
        value = val.substring(0, val.offsetByCodePoints(0, maxLength));
      }
    }
    return value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (o == null || getClass() != o.getClass())
      return false;

    AbstractChar<T> that = (AbstractChar<T>) o;

    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public String toString() {
    return value;
  }

  public int length() {
    return value.length();
  }
}
