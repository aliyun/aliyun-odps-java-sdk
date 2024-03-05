package com.aliyun.odps.type;

import java.util.Objects;

import com.aliyun.odps.OdpsType;

/**
 *
 * Created by zhenhong.gzh on 16/7/11.
 */
public abstract class AbstractCharTypeInfo extends AbstractPrimitiveTypeInfo {

  private int length;

  AbstractCharTypeInfo(OdpsType type, int length) {
    super(type);
    validateParameter(length);
    this.length = length;
  }

  /**
   * 验证精度的有效性
   *
   * @param length
   *     精度
   */
  protected abstract void validateParameter(int length);

  /**
   * 获取精度
   *
   * @return 精度
   */
  public int getLength() {
    return length;
  }


  @Override
  public String getTypeName() {
    return String.format("%s(%s)", super.getTypeName(), length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AbstractCharTypeInfo that = (AbstractCharTypeInfo) o;
    return length == that.length;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), length);
  }
}
