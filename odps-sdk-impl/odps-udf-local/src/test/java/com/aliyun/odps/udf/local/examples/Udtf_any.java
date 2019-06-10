package com.aliyun.odps.udf.local.examples;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"any,*->boolean,*"})
public class Udtf_any extends UDTF {

  @Override
  public void process(Object[] args) throws UDFException {
    String a = (args[0] == null) ? null : (String) args[0];
    String b = (args[1] == null) ? null : (String) args[1];
    boolean bool = a != null && a.equals(b);
    forward(bool, a, b);
  }

}
