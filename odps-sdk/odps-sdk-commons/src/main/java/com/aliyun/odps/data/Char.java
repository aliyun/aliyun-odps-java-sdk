package com.aliyun.odps.data;

/**
 * Char 类型对应的数据类
 *
 * Created by zhenhong.gzh on 16/12/12.
 */

public class Char extends AbstractChar implements Comparable<Char> {
  public Char(String value) {
    super(value);
  }

  public Char(String value, int length) {
    super(value, length);
  }

  @Override
  public int compareTo(Char rhs) {
    if (rhs == this) {
      return 0;
    }
    return value.compareTo(rhs.value);
  }
}
