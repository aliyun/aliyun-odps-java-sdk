package com.aliyun.odps.udf.local.examples;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import java.util.ArrayList;
import java.util.List;

@Resolve({"string->string,array<string>"})
public class Udtf_complex extends UDTF {

  @Override
  public void process(Object[] args) throws UDFException {
    String a = (String) args[0];
    List list = new ArrayList();
    list.add(a + "x");
    list.add(a + "y");
    forward(a, list);

  }
}