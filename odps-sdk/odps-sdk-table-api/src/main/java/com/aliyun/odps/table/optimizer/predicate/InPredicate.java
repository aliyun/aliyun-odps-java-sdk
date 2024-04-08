package com.aliyun.odps.table.optimizer.predicate;

import java.io.Serializable;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class InPredicate extends Predicate {

  public enum Operator {
    /**
     * IN and NOT IN operators for set membership check
     */
    IN("in"),
    NOT_IN("not in");

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
  private final List<Serializable> set;

  public InPredicate(Operator operator, Serializable operand, List<Serializable> set) {
    super(PredicateType.IN);
    this.operator = operator;
    this.operand = operand;
    this.set = set;
  }

  public static InPredicate in(Serializable operand, List<Serializable> set) {
    return new InPredicate(Operator.IN, operand, set);
  }

  public static InPredicate notIn(Serializable operand, List<Serializable> set) {
    return new InPredicate(Operator.NOT_IN, operand, set);
  }

  public Operator getOperator() {
    return operator;
  }

  public Object getOperand() {
    return operand;
  }

  public List<Serializable> getSet() {
    return set;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(operand).append(" ").append(operator.getDescription()).append(" (");
    for (int i = 0; i < set.size(); i++) {
      sb.append(set.get(i).toString());
      if (i < set.size() - 1) {
        sb.append(", ");
      }
    }
    sb.append(")");
    return sb.toString();
  }
}