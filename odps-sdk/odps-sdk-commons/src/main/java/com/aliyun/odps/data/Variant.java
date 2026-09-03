package com.aliyun.odps.data;

import java.io.Serializable;
import java.util.Objects;

public class Variant implements Serializable {

  private final String value;

  public Variant(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Variant variant = (Variant) o;
    return Objects.equals(value, variant.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
}
