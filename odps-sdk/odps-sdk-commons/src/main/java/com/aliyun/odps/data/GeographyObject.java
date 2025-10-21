package com.aliyun.odps.data;

import java.io.Serializable;

public interface GeographyObject extends Serializable {
  java.nio.ByteBuffer asBinary();
  String asText();
}
