package com.aliyun.odps.sqa.commandapi.utils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsLexer;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser.FromClauseContext;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser.FromRestContext;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser.FromStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser.InsertStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser.MultiInsertBranchContext;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser.QueryStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser.SelectQueryStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParser.WithClauseContext;
import com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsParserBaseListener;

public class SqlParserUtil {

  static class SqlParserListener extends OdpsParserBaseListener {

    /**
     * queryStatement及其分支
     */
    private boolean queryStatement;
    private boolean withClause;
    private boolean selectQueryStatement;
    private boolean fromStatement;
    private boolean insertStatement;
    private boolean explainStatement;

    /**
     * fromStatement的分支
     */
    private boolean fromClause;
    private boolean fromRest;
    private boolean multiInsertBranch;

    @Override
    public void exitQueryStatement(QueryStatementContext ctx) {
      queryStatement = true;
    }

    @Override
    public void exitWithClause(WithClauseContext ctx) {
      withClause = true;
    }

    @Override
    public void exitSelectQueryStatement(SelectQueryStatementContext ctx) {
      selectQueryStatement = true;
    }

    @Override
    public void exitFromStatement(FromStatementContext ctx) {
      fromStatement = true;
    }

    @Override
    public void exitInsertStatement(InsertStatementContext ctx) {
      insertStatement = true;
    }

    @Override
    public void exitFromClause(FromClauseContext ctx) {
      fromClause = true;
    }

    @Override
    public void exitFromRest(FromRestContext ctx) {
      fromRest = true;
    }

    @Override
    public void enterWithClause(WithClauseContext ctx) {
      withClause = true;
    }

    @Override
    public void enterExplainStatement(OdpsParser.ExplainStatementContext ctx) {
      explainStatement = true;
    }

    @Override
    public void exitExplainStatement(OdpsParser.ExplainStatementContext ctx) {
      explainStatement = true;
    }

    @Override
    public void exitMultiInsertBranch(MultiInsertBranchContext ctx) {
      multiInsertBranch = true;
    }

    public boolean isQueryStatement() {
      return queryStatement;
    }

    public boolean isWithClause() {
      return withClause;
    }

    public boolean isSelectQueryStatement() {
      return selectQueryStatement;
    }

    public boolean isFromStatement() {
      return fromStatement;
    }

    public boolean isInsertStatement() {
      return insertStatement;
    }

    public boolean isFromClause() {
      return fromClause;
    }

    public boolean isFromRest() {
      return fromRest;
    }

    public boolean isMultiInsertBranch() {
      return multiInsertBranch;
    }

    public boolean isExplainStatement() {
      return explainStatement;
    }
  }

  /**
   * 通过语法树判断sql是否具备结果集
   *
   * @param sql sql语句
   * @return
   */
  public static boolean hasResultSet(String sql) {
    SqlParserListener parserListener = getSqlParserListener(sql);

    if (parserListener.isQueryStatement()) {
      if (parserListener.isSelectQueryStatement()) {
        return true;
      }

      if (parserListener.isInsertStatement()) {
        return false;
      }

      if (parserListener.isFromStatement()) {
        if (parserListener.isFromRest()) {
          return true;
        } else if (parserListener.isMultiInsertBranch()) {
          return false;
        } else {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * 判断sql是否是select
   * 非select:
   * 1. explain select
   * 2. from xxx insert
   * select:
   * 1. from xxx select
   * 2. with xxx as xxx select
   */
  public static boolean isSelect(String sql) throws SQLException {
    SqlParserListener parserListener = getSqlParserListener(sql);

    if (parserListener.isQueryStatement()) {
      if (parserListener.isSelectQueryStatement()) {
        if (parserListener.isExplainStatement()) {
          return false;
        }
        return true;
      }

      if (parserListener.isInsertStatement()) {
        return false;
      }

      if (parserListener.isFromStatement()) {
        if (parserListener.isFromRest()) {
          return true;
        } else {
          return !parserListener.isMultiInsertBranch();
        }
      }
    }
    return false;
  }

  /**
   * 获取query里占位符'?'的位置，忽略常量字符'?'。主要用于preparedStatement
   *
   * @param query sql
   * @return
   */
  public static List<Integer> getPlaceholderIndexList(String query) {
    List<Integer> res = new ArrayList<>();
    ANTLRInputStream input = new ANTLRInputStream(query);
    OdpsLexer lexer = new OdpsLexer(input);
    for (Token token : lexer.getAllTokens()) {
      if (token != null && token.getType() == OdpsLexer.QUESTION) {
        res.add(token.getStartIndex());
      }
    }
    return res;
  }

  /**
   * 采用的语法树是odps-sql的语法树
   *
   * @param sql sql语句
   * @return
   */
  private static SqlParserListener getSqlParserListener(String sql) {
    ANTLRInputStream input = new ANTLRInputStream(sql);
    OdpsLexer lexer = new OdpsLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    OdpsParser parser = new OdpsParser(tokens);

    SqlParserListener parserListener = new SqlParserListener();
    ParseTreeWalker treeWalker = new ParseTreeWalker();
    treeWalker.walk(parserListener, parser.script());
    return parserListener;
  }

}
