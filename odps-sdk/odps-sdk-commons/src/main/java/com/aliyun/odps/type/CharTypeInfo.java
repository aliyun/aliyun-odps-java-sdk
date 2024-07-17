package com.aliyun.odps.type;

import com.aliyun.odps.OdpsType;

/**
 * Odps char 类型
 *
 * Created by zhenhong.gzh on 16/7/12.
 */
public class CharTypeInfo extends AbstractCharTypeInfo {
  private static final long serialVersionUID = 1L;
  static final int MAX_CHAR_LENGTH = 0xff;

  /**
   * 创建 char 类型
   *
   * @param length
   *      char 的精度
   */
  CharTypeInfo(int length) {
    super(OdpsType.CHAR, length);
  }

  @Override
  protected void validateParameter(int length) {
    if (length < 1 || length > MAX_CHAR_LENGTH) {
      throw new IllegalArgumentException("Char length " + length + " out of range [1, " + MAX_CHAR_LENGTH + "]");
    }
  }

}
