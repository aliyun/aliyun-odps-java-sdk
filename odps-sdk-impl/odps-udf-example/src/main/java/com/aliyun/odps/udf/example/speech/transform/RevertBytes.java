package com.aliyun.odps.udf.example.speech.transform;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.udf.UDF;
import org.apache.commons.lang.ArrayUtils;

public class RevertBytes extends UDF {
  public Binary evaluate(Binary bytes) {
    ArrayUtils.reverse(bytes.data());
    return bytes;
  }
}