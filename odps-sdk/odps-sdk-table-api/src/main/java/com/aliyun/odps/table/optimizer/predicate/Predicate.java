package com.aliyun.odps.table.optimizer.predicate;

import java.io.Serializable;
import java.util.Objects;

import com.aliyun.odps.utils.StringUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 * 开放存储支持谓词下推所使用的谓词
 * 注意目前为区分变量（列名）和常量，因此当处理String类型常量时，需要使用端自行处理引号，处理其他类型常量，也需要自行重写toString方法
 * <p>
 * 谓词下推并不意味着扫描数据量一定会减少，而是通过谓词尽可能的过滤数据桶
 * 只能保证扫描数据中一定包含满足谓词的桶
 */
public abstract class Predicate implements Serializable {

  public static final Predicate NO_PREDICATE = new Predicate(PredicateType.RAW) {
    @Override
    public String toString() {
      return "";
    }
  };

  /**
   * 定义谓词的类型
   */
  public enum PredicateType {
    /**
     * 二元谓词
     * such as AND, OR, NOT
     */
    BINARY,
    /**
     * 一元谓词
     * IS_NULL, IS_NOT_NULL
     */
    UNARY,
    /**
     * 复合谓词
     * AND, OR
     */
    COMPOUND,
    /**
     * IN, NOT IN
     */
    IN,
    /**
     * 常量
     */
    CONSTANT,
    /**
     * String类型，不进行任何处理
     */
    RAW
  }

  private final PredicateType type;

  protected Predicate(PredicateType type) {
    this.type = type;
  }

  public PredicateType getType() {
    return type;
  }

  /**
   * 要求所有子类实现toString方法，以便输出谓词的字符串表示
   *
   * @return 传给服务端的谓词
   */
  @Override
  public abstract String toString();

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Predicate)) {
      return false;
    }
    return o.toString().equals(toString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(toString());
  }

  protected boolean validatePredicate(Predicate predicate) {
    return predicate != null && !StringUtils.isNullOrEmpty(predicate.toString());
  }
}
