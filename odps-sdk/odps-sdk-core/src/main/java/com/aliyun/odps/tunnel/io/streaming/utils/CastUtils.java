package com.aliyun.odps.tunnel.io.streaming.utils;

import java.nio.charset.StandardCharsets;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.tunnel.TunnelException;

// TODO
public class CastUtils {

  /**
   * 用于将record value转为字节数组
   */
  public static byte[] toByteArray(Object value, OdpsType type) throws TunnelException {
    switch (type) {
      case BIGINT:
        return String.valueOf((long) value).getBytes(StandardCharsets.UTF_8);
      case STRING:
        return ((String) value).getBytes(StandardCharsets.UTF_8);
      case CHAR:
        return String.valueOf(((char) value)).getBytes(StandardCharsets.UTF_8);
      case INT:
        return String.valueOf((int) value).getBytes(StandardCharsets.UTF_8);
      default:
        throw new TunnelException("This type cannot get bytes: " + type);
    }
  }
}
