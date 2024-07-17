package com.aliyun.odps.type;

import java.io.Serializable;

import com.aliyun.odps.OdpsType;

/**
 * Odps 类型抽象类
 *
 * Created by zhenhong.gzh on 16/7/7.
 */
public interface TypeInfo extends Serializable {
  String getTypeName();

  OdpsType getOdpsType();
}
