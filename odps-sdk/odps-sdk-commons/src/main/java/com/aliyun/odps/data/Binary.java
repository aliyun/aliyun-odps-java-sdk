package com.aliyun.odps.data;

import java.io.Serializable;
import java.util.Arrays;

import com.aliyun.odps.utils.StringUtils;

/**
 * Binary 类型对应的数据类
 *
 * Created by zhenhong.gzh on 16/12/12.
 */
public class Binary implements Comparable<Binary>, Serializable {
  protected byte[] data;

  public Binary(byte[] data) {
    this.data = data;
  }

  public byte[] data() {
    return data;
  }

  public int length() {
    return data.length;
  }

  @Override
  public int compareTo(Binary rhs) {
    int len1 = data.length;
    int len2 = rhs.data.length;
    int lim = Math.min(len1, len2);
    byte[] v1 = data;
    byte[] v2 = rhs.data;

    for (int i = 0; i<lim; ++i) {
      byte b1 = v1[i];
      byte b2 = v2[i];
      if (b1 != b2) {
        return b1 - b2;
      }
    }
    return len1 - len2;
  }

  @Override
  public String toString() {
    return new String(StringUtils.encodeQuotedPrintable(data));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Binary binary = (Binary) o;

    return Arrays.equals(data, binary.data);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(data);
  }
}
