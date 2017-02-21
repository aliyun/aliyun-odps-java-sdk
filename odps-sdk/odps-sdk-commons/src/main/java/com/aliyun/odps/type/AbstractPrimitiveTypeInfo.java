package com.aliyun.odps.type;

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
}
