package com.aliyun.odps.data;


/**
 * Varchar 类型对应的数据类
 *
 * Created by zhenhong.gzh on 16/12/12.
 */

public class Varchar extends AbstractChar implements Comparable<Varchar> {
  public Varchar(String value) {
    super(value);
  }

  public Varchar(String value, int length) {
    super(value, length);
  }

  @Override
  public int compareTo(Varchar rhs) {
    if (rhs == this) {
      return 0;
    }
    return value.compareTo(rhs.value);
  }
}
