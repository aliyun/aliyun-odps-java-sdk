package com.aliyun.odps.udf;

import org.apache.arrow.gandiva.expression.TreeNode;

public class RowExpression {
    private TreeNode expr;

    public TreeNode getRemainingExpr() {
        return expr;
    }

    public void setRemainingExpr(TreeNode expr) {
        this.expr = expr;
    }
}
