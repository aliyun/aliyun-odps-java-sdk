package com.aliyun.odps.table.optimizer.predicate;

import java.io.Serializable;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class UnaryPredicate extends Predicate {

  public enum Operator {
    /**
     * 一元运算操作符
     */
    IS_NULL("is null"),
    NOT_NULL("is not null");

    private final String description;

    Operator(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }
  }

  private final Operator operator;
  private final Serializable operand;

  public UnaryPredicate(Operator operator, Serializable operand) {
    super(PredicateType.UNARY);
    this.operator = operator;
    this.operand = operand;
  }

  public static UnaryPredicate isNull(Serializable operand) {
    return new UnaryPredicate(Operator.IS_NULL, operand);
  }

  public static UnaryPredicate notNull(Serializable operand) {
    return new UnaryPredicate(Operator.NOT_NULL, operand);
  }

  public Operator getOperator() {
    return operator;
  }

  public Object getOperand() {
    return operand;
  }

  @Override
  public String toString() {
    return operand.toString() + " " + operator.getDescription();
  }
}
