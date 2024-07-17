package com.aliyun.odps.type;

import com.aliyun.odps.OdpsType;

/**
 * odps 基本类型
 *
 * Created by zhenhong.gzh on 16/7/7.
 */
public class SimplePrimitiveTypeInfo extends AbstractPrimitiveTypeInfo {
  private static final long serialVersionUID = 1L;
  SimplePrimitiveTypeInfo(OdpsType odpsType) {
    super(odpsType);
  }
}
