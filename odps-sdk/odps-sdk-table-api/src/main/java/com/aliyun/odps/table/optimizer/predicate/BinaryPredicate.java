package com.aliyun.odps.table.optimizer.predicate;

import java.io.Serializable;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class BinaryPredicate extends Predicate {

  public enum Operator {
    /**
     * 二元运算操作符
     */
    EQUALS("="),
    NOT_EQUALS("!="),
    GREATER_THAN(">"),
    LESS_THAN("<"),
    GREATER_THAN_OR_EQUAL(">="),
    LESS_THAN_OR_EQUAL("<="),
    LIKE("LIKE");

    private final String description;

    Operator(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }
  }

  private final Operator operator;
  private final Serializable leftOperand;
  private final Serializable rightOperand;

  public BinaryPredicate(Operator operator, Serializable leftOperand, Serializable rightOperand) {
    super(PredicateType.BINARY);
    this.operator = operator;
    this.leftOperand = leftOperand;
    this.rightOperand = rightOperand;
  }

  public static BinaryPredicate equals(Serializable leftOperand, Serializable rightOperand) {
    return new BinaryPredicate(Operator.EQUALS, leftOperand, rightOperand);
  }

  public static BinaryPredicate notEquals(Serializable leftOperand, Serializable rightOperand) {
    return new BinaryPredicate(Operator.NOT_EQUALS, leftOperand, rightOperand);
  }

  public static BinaryPredicate greaterThan(Serializable leftOperand, Serializable rightOperand) {
    return new BinaryPredicate(Operator.GREATER_THAN, leftOperand, rightOperand);
  }

  public static BinaryPredicate lessThan(Serializable leftOperand, Serializable rightOperand) {
    return new BinaryPredicate(Operator.LESS_THAN, leftOperand, rightOperand);
  }

  public static BinaryPredicate greaterThanOrEqual(Serializable leftOperand,
                                                   Serializable rightOperand) {
    return new BinaryPredicate(Operator.GREATER_THAN_OR_EQUAL, leftOperand, rightOperand);
  }

  public static BinaryPredicate lessThanOrEqual(Serializable leftOperand,
                                                Serializable rightOperand) {
    return new BinaryPredicate(Operator.LESS_THAN_OR_EQUAL, leftOperand, rightOperand);
  }

  public static BinaryPredicate like(Serializable leftOperand, Serializable rightOperand) {
    return new BinaryPredicate(Operator.LIKE, leftOperand, rightOperand);
  }

  public Operator getOperator() {
    return operator;
  }

  public Object getLeftOperand() {
    return leftOperand;
  }

  public Object getRightOperand() {
    return rightOperand;
  }

  @Override
  public String toString() {
    return leftOperand.toString() + " " + operator.getDescription() + " " + rightOperand.toString();
  }
}
