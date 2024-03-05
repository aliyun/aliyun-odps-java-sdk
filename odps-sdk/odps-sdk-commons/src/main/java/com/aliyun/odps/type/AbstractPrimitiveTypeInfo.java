package com.aliyun.odps.type;

import java.util.Objects;

import com.aliyun.odps.OdpsType;

/**
 * odps 基本类型接口
 *
 * Created by zhenhong.gzh on 16/7/7.
 */
public abstract class AbstractPrimitiveTypeInfo implements PrimitiveTypeInfo {
  private OdpsType odpsType;

  AbstractPrimitiveTypeInfo() {

  }

  AbstractPrimitiveTypeInfo(OdpsType type) {
    this.odpsType = type;
  }

  @Override
  public String getTypeName() {
    return odpsType.name();
  }

  @Override
  public OdpsType getOdpsType() {
    return odpsType;
  }

  @Override
  public String toString() {
    return getTypeName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractPrimitiveTypeInfo that = (AbstractPrimitiveTypeInfo) o;
    return odpsType == that.odpsType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(odpsType);
  }
}
