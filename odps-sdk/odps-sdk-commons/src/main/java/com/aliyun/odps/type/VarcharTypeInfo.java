package com.aliyun.odps.type;

import com.aliyun.odps.OdpsType;

/**
 * Odps varchar 类型
 *
 * Created by zhenhong.gzh on 16/7/12.
 */
public class VarcharTypeInfo extends AbstractCharTypeInfo {
  private static final long serialVersionUID = 1L;
  static final int MAX_VARCHAR_LENGTH = 0xffff;
  /**
   * 创建 varchar 类型
   *
   * @param length
   *      varchar 的精度
   */
  public VarcharTypeInfo(int length) {
    super(OdpsType.VARCHAR, length);
  }

  @Override
  protected void validateParameter(int length) {
    if (length < 1 || length > MAX_VARCHAR_LENGTH) {
      throw new IllegalArgumentException("Varchar length " + length + " out of range [1, " + MAX_VARCHAR_LENGTH + "]");
    }
  }
}
