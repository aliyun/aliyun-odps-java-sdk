package com.aliyun.odps.expression;

import java.lang.reflect.Type;
import java.util.List;

import com.aliyun.odps.data.GenerateExpression;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class GenerationExpressionParser {

  /**
   * trunc_time(d, 'day') -->
   * [
   * {
   * "leafExprDesc": {
   * "reference": {
   * "name": "d"
   * },
   * "type": "timestamp"
   * }
   * },
   * {
   * "leafExprDesc": {
   * "constant": "day",
   * "type": "string"
   * }
   * },
   * {
   * "functionCall": {
   * "name": "trunc_time",
   * "type": "string"
   * }
   * }
   * ]
   */
  public static GenerateExpression parse(String exprJson) {
    if (StringUtils.isNullOrEmpty(exprJson)) {
      return null;
    }
    List<ExpressionItem> generateExpression = parseFromJson(exprJson);

    // Since the server uses post-order traversal to create the list,
    // the last item in the list is always the top-level Expression.
    ExpressionItem topLevelExpression = generateExpression.get(generateExpression.size() - 1);
    FunctionCall functionCall = topLevelExpression.getFunctionCall();
    // We can parse the Expression into a tree later.
    // Since there are only a few simple Expressions now, we can just parse it with hard code.
    if (functionCall != null && TruncTime.NAME.equalsIgnoreCase(functionCall.getName())) {
      return new TruncTime(generateExpression.get(0).getLeafExprDesc().getReference().getName(),
                           generateExpression.get(1).getLeafExprDesc().getConstant());
    } else {
      throw new UnsupportedOperationException(
          "Unknown Expression: " + (functionCall != null ? functionCall.getName() : "null"));
    }
  }

  private static final Gson GSON = new Gson();
  private static final Type EXPRESSION_LIST_TYPE = new TypeToken<List<ExpressionItem>>() {
  }.getType();

  private static List<ExpressionItem> parseFromJson(String exprJson) {
    return GSON.fromJson(exprJson, EXPRESSION_LIST_TYPE);
  }


  static class ExpressionItem {

    private LeafExprDesc leafExprDesc;
    private FunctionCall functionCall;

    public LeafExprDesc getLeafExprDesc() {
      return leafExprDesc;
    }

    public void setLeafExprDesc(LeafExprDesc leafExprDesc) {
      this.leafExprDesc = leafExprDesc;
    }

    public FunctionCall getFunctionCall() {
      return functionCall;
    }

    public void setFunctionCall(FunctionCall functionCall) {
      this.functionCall = functionCall;
    }
  }

  public static class LeafExprDesc {

    public static class Reference {

      private String name;

      public String getName() {
        return name;
      }

      public void setName(String name) {
        this.name = name;
      }
    }

    private Reference reference;
    private String constant;
    private boolean isNull;
    private String type;

    public Reference getReference() {
      return reference;
    }

    public void setReference(Reference reference) {
      this.reference = reference;
    }

    public String getConstant() {
      return constant;
    }

    public void setConstant(String constant) {
      this.constant = constant;
    }

    public boolean isNull() {
      return isNull;
    }

    public void setNull(boolean aNull) {
      isNull = aNull;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }
  }

  static class FunctionCall {

    private String name;
    private Integer parameterCount;
    private String type;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Integer getParameterCount() {
      return parameterCount;
    }

    public void setParameterCount(Integer parameterCount) {
      this.parameterCount = parameterCount;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }
  }
}
