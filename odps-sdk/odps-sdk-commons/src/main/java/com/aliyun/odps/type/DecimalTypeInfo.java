package com.aliyun.odps.type;

import java.util.Objects;

import com.aliyun.odps.OdpsType;

/**
 * Odps decimal 类型
 *
 * Created by zhenhong.gzh on 16/7/7.
 */
public class DecimalTypeInfo extends AbstractPrimitiveTypeInfo {
  private static final long serialVersionUID = 1L;
  static final int DEFAULT_PRECISION = 54;
  static final int DEFAULT_SCALE = 18;

  private final int precision;
  private final int scale;

  DecimalTypeInfo() {
    this(DEFAULT_PRECISION, DEFAULT_SCALE);
  }

  /**
   * 创建 odps decimal 类型
   *
   * @param precision
   *      精度
   * @param scale
   *      小数点后保留位数
   */
  DecimalTypeInfo(int precision, int scale) {
    super(OdpsType.DECIMAL);

    validateParameter(precision, scale);
    this.precision = precision;
    this.scale = scale;
  }

  private void validateParameter(int precision, int scale) {
    if (precision < 1) {
      throw new IllegalArgumentException("Decimal precision < 1");
    }

    if (scale < 0) {
      throw new IllegalArgumentException("Decimal scale < 0");
    }

    if (scale > precision) {
      throw new IllegalArgumentException("Decimal precision must be larger than or equal to scale");
    }
  }

  @Override
  public String getTypeName() {
    if ((precision == DEFAULT_PRECISION) && (scale == DEFAULT_SCALE)) {
      return super.getTypeName();
    }

    return String.format("%s(%s,%s)", super.getTypeName(), precision, scale);
  }

  /**
   * 获取精度
   *
   * @return 精度
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * 获取小数点后位数
   * @return 小数点后位数
   */
  public int getScale() {
    return scale;
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
    DecimalTypeInfo that = (DecimalTypeInfo) o;
    return precision == that.precision && scale == that.scale;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), precision, scale);
  }
}
