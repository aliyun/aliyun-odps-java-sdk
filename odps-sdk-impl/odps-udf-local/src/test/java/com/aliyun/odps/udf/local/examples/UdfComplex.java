package com.aliyun.odps.udf.local.examples;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.udf.UDF;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class UdfComplex extends UDF {

  public String evaluate(Byte b) {
    return "Byte:" + b;
  }
  public String evaluate(Short s) {
    return "Short:" + s;
  }
  public String evaluate(Integer i) {
    return "Integer:" + i;
  }
  public String evaluate(Long l) {
    return "Long:" + l;
  }
  public String evaluate(Float f) {
    return "Float:" + f;
  }
  public String evaluate(Double d) {
    return "Double:" + d;
  }
  public String evaluate(BigDecimal b) {
    return "BigDecimal:" + b;
  }
  public String evaluate(Boolean b) {
    return "Boolean:" + b;
  }
  public String evaluate(Varchar v) {
    return "Varchar:" + v;
  }
  public String evaluate(Binary b) {
    return "Binary:" + b;
  }
  public String evaluate(Date d) {
    return "Date:" + d;
  }
  public String evaluate(Timestamp t) {
    return "Timestamp:" + t;
  }
  public String evaluate(List list) {
    return "List:" + list;
  }
  public String evaluate(Map map) {
    return "Map:" + map;
  }
  public String evaluate(Struct struct) {
    return "Struct:" + struct.getFieldValues().toString();
  }

}
