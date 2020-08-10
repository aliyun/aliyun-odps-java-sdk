package com.aliyun.odps.udf.local.examples;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve({"any,*->boolean,*"})
public class Udtf_any extends UDTF {

  @Override
  public void process(Object[] args) throws UDFException {
    Object[] result = new Object[args.length + 1];
    boolean allSame = true;
    String o = null;
    for (int i = 0; i < args.length; i++) {
      String current = (String) args[i];
      result[i + 1] = current;
      if (i == 0) {
        o = current;
      } else {
        if (!isSame(current, o)) {
          allSame = false;
        }
      }
    }
    result[0] = allSame;
    forward(result);
  }

  private boolean isSame(String l, String r) {
    return l == null ? r == null : l.equals(r);
  }

}
