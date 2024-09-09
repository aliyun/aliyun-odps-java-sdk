package com.aliyun.odps.table.optimizer.predicate;

import java.io.Serializable;
import java.util.Objects;

import com.aliyun.odps.data.converter.OdpsRecordConverter;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.OdpsCommonUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Constant extends Predicate {

  private final Serializable value;
  private final TypeInfo typeInfo;
  private static final OdpsRecordConverter formatter;

  static {
    formatter = OdpsRecordConverter.builder()
        .setStrictMode(false)
        .timezone("UTC")
        .enableSqlStandardFormat()
        .build();
  }

  public Constant(Object value) {
    this(value, OdpsCommonUtils.indicateTypeFromClass(value));
  }

  public Constant(Object value, TypeInfo typeInfo) {
    super(PredicateType.CONSTANT);
    this.value = (Serializable) value;
    this.typeInfo = typeInfo;
  }

  public static Constant of(Object value) {
    return new Constant(value);
  }

  public static Constant of(Object value, TypeInfo typeInfo) {
    return new Constant(value, typeInfo);
  }

  @Override
  public String toString() {
    try {
      return formatter.formatObject(value, typeInfo);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Invalid constant value: " + value.toString() + "[" + value.getClass().getName() + "]"
          + " for type " + typeInfo.getTypeName()
          + ". You can use RawPredicate to handle constant manually if you think this method does not handle your input appropriately. ",
          e);
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
    Constant constant = (Constant) o;
    return Objects.equals(value, constant.value) && Objects.equals(typeInfo, constant.typeInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), value);
  }
}
