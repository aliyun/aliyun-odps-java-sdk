package com.aliyun.odps.local.common;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;

public class AnyTypeInfo implements TypeInfo {
  private static final long serialVersionUID = 1L;

  @Override
  public String getTypeName() {
    return "ANY";
  }

  @Override
  public OdpsType getOdpsType() {
    return null;
  }

}