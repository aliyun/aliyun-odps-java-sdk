package com.aliyun.odps.sqa.commandapi.utils;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.aliyun.odps.compiler.parser.OdpsLexer;
import com.aliyun.odps.compiler.parser.OdpsParser;
import com.aliyun.odps.compiler.parser.OdpsParser.FromClauseContext;
import com.aliyun.odps.compiler.parser.OdpsParser.FromRestContext;
import com.aliyun.odps.compiler.parser.OdpsParser.FromStatementContext;
import com.aliyun.odps.compiler.parser.OdpsParser.InsertStatementContext;
import com.aliyun.odps.compiler.parser.OdpsParser.MultiInsertBranchContext;
import com.aliyun.odps.compiler.parser.OdpsParser.QueryStatementContext;
import com.aliyun.odps.compiler.parser.OdpsParser.SelectQueryStatementContext;
import com.aliyun.odps.compiler.parser.OdpsParser.WithClauseContext;
import com.aliyun.odps.compiler.parser.OdpsParserBaseListener;

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
