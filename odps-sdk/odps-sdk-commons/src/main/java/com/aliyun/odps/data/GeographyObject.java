package com.aliyun.odps.data;

import java.io.Serializable;

public interface GeographyObject extends Serializable {
  byte[] asBinary();
  String asText();
}
