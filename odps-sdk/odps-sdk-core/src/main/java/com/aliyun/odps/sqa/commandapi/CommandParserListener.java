/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.sqa.commandapi;


import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.AlterStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.AlterTableStatementSuffixContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.AuthorizationStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.DescInstanceStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.DescProjectStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.DescTableExtendedStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.DescTableStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.PartitionSpecContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.ShowInstanceStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.ShowPartitionStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.ShowTableStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.ShowStmtIdentifierContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.SqlCostStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.TableNameContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParser.WhoamiStatementContext;
import com.aliyun.odps.sqa.commandapi.antlr.command.CommandParserBaseListener;


public class CommandParserListener extends CommandParserBaseListener {

  private Command command;

  @Override
  public void exitAlterStatement(AlterStatementContext ctx) {
    TableNameContext tableNameContext = ctx.tableNamee;
    if (tableNameContext == null) {
      return;
    }
    String tableName = tableNameContext.getText();

    PartitionSpecContext partitionSpecContext = ctx.partition;
    String partition = "";
    if (partitionSpecContext != null) {
      partition = partitionSpecContext.getText();
    }

    AlterTableStatementSuffixContext alterCtx = ctx.alterTableStatementSuffix();
    if (alterCtx == null) {
      return;
    }

    if (alterCtx.archive != null) {
      command = new ArchiveCommand(tableName, partition);
    } else if (alterCtx.merge != null) {
      command = new MergeCommand(tableName, partition);
    } else if (alterCtx.compact != null) {
      if (alterCtx.compact.compactTypee == null) {
        return;
      }
      String compactType = alterCtx.compact.compactTypee.getText();
      command = new CompactCommand(tableName, partition, compactType);
    } else if (alterCtx.freeze != null) {
      command = new FreezeCommand(tableName, partition);
    } else if (alterCtx.restore != null) {
      command = new RestoreCommand(tableName, partition);
    }
  }

  @Override
  public void exitShowTableStatement(ShowTableStatementContext ctx) {
    String projectName = null;
    String schemaName = null;
    String prefixName = null;

    if (ctx.project_Name != null) {
      projectName = unquoteIdentifier(ctx.project_Name.getText());
    }
    if (ctx.schema_Name != null) {
      schemaName = unquoteIdentifier(ctx.schema_Name.getText());
    }
    if (ctx.prefix_name != null) {
      prefixName = unquoteShowStmtIdentifier(ctx.prefix_name);
    }

    command = new ShowTablesCommand(projectName, schemaName, prefixName);
  }

  @Override
  public void exitAuthorizationStatement(AuthorizationStatementContext ctx) {
    command = new AuthorizationCommand();
  }

  @Override
  public void exitSqlCostStatement(SqlCostStatementContext ctx) {
    String query = null;
    // 直接getText会跳过空格 https://github.com/antlr/antlr4/issues/1838
    if (ctx.query != null) {
      Token queryStart = ctx.query.start;
      Token queryStop = ctx.query.stop;
      Interval interval = new Interval(queryStart.getStartIndex(), queryStop.getStopIndex());
      query = queryStart.getInputStream().getText(interval);
    }
    command = new SQLCostCommand(query);
  }

  @Override
  public void exitShowPartitionStatement(ShowPartitionStatementContext ctx) {
    String projectName = null;
    String schemaName = null;
    String tableName = null;
    if (ctx.table_Name != null) {
      TableNameContext tableCtx = ctx.table_Name;
      if (tableCtx.db != null) {
        projectName = tableCtx.db.getText();
      }
      if (tableCtx.sch != null) {
        schemaName = tableCtx.sch.getText();
      }
      if (tableCtx.tab != null) {
        tableName = tableCtx.tab.getText();
      }
    }

    StringBuilder sb = new StringBuilder();
    if (ctx.partition_Spec != null && ctx.partition_Spec.partitions != null) {
      for (int i = 0; i < ctx.partition_Spec.partitions.size(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        sb.append(ctx.partition_Spec.partitions.get(i).getText());
      }
    }

    String partition = sb.toString().equals("") ? null : sb.toString();

    command = new ShowPartitionsCommand(projectName, schemaName, tableName, partition);
  }

  @Override
  public void exitShowInstanceStatement(ShowInstanceStatementContext ctx) {
    String fromDate = null;
    String toDate = null;
    String number = null;

    if (ctx.from_date != null) {
      fromDate = ctx.from_date.getText();
    }

    if (ctx.to_date != null) {
      toDate = ctx.to_date.getText();
    }

    if (ctx.Num() != null) {
      number = ctx.Num().getText();
    }

    command = new ShowInstancesCommand(fromDate, toDate, number);
  }

  @Override
  public void exitDescTableStatement(DescTableStatementContext ctx) {
    String projectName = null;
    String schemaName = null;
    String tableName = null;
    if (ctx.table_Name != null) {
      TableNameContext tableCtx = ctx.table_Name;
      if (tableCtx.db != null) {
        projectName = tableCtx.db.getText();
      }
      if (tableCtx.sch != null) {
        schemaName = tableCtx.sch.getText();
      }
      if (tableCtx.tab != null) {
        tableName = tableCtx.tab.getText();
      }
    }

    StringBuilder sb = new StringBuilder();
    if (ctx.partition_Spec != null && ctx.partition_Spec.partitions != null) {
      for (int i = 0; i < ctx.partition_Spec.partitions.size(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        sb.append(ctx.partition_Spec.partitions.get(i).getText());
      }
    }

    String partition = sb.toString().equals("") ? null : sb.toString();

    command = new DescribeTableCommand(projectName, schemaName, tableName, partition, false);
  }

  @Override
  public void exitDescTableExtendedStatement(DescTableExtendedStatementContext ctx) {
    String projectName = null;
    String schemaName = null;
    String tableName = null;
    if (ctx.table_Name != null) {
      TableNameContext tableCtx = ctx.table_Name;
      if (tableCtx.db != null) {
        projectName = tableCtx.db.getText();
      }
      if (tableCtx.sch != null) {
        schemaName = tableCtx.sch.getText();
      }
      if (tableCtx.tab != null) {
        tableName = tableCtx.tab.getText();
      }
    }

    StringBuilder sb = new StringBuilder();
    if (ctx.partition_Spec != null && ctx.partition_Spec.partitions != null) {
      for (int i = 0; i < ctx.partition_Spec.partitions.size(); i++) {
        if (i != 0) {
          sb.append(",");
        }
        sb.append(ctx.partition_Spec.partitions.get(i).getText());
      }
    }

    String partition = sb.toString().equals("") ? null : sb.toString();

    command = new DescribeTableCommand(projectName, schemaName, tableName, partition, true);
  }

  @Override
  public void exitDescProjectStatement(DescProjectStatementContext ctx) {
    String projectName = null;
    boolean extended = false;

    if (ctx.project_Name != null) {
      projectName = ctx.project_Name.getText();
    }

    if (ctx.KW_EXTENDED() != null) {
      extended = true;
    }

    command = new DescribeProjectCommand(projectName, extended);
  }

  @Override
  public void exitDescInstanceStatement(DescInstanceStatementContext ctx) {
    String instanceId = null;
    if (ctx.instance_Id != null) {
      instanceId = ctx.instance_Id.getText();
    }
    command = new DescribeInstanceCommand(instanceId);
  }

  @Override
  public void exitWhoamiStatement(WhoamiStatementContext ctx) {
    command = new WhoamiCommand();
  }


  @Override
  public void exitShowCreateTableStatement(CommandParser.ShowCreateTableStatementContext ctx) {
    command = new ShowCreateTableCommand();
  }

  @Override
  public void exitShowSchemasStatament(CommandParser.ShowSchemasStatamentContext ctx) {
    command = new ShowSchemasCommand();
  }

  @Override
  public void exitDescSchemaStatement(CommandParser.DescSchemaStatementContext ctx) {
    command = new DescribeSchemaCommand();
  }

  /**
   * `showStmtIdentifier` accepts a bare identifier as well as a quoted string literal, while the
   * value ends up as the `name` parameter of the list tables request, which is a table name
   * prefix. Raw token text still carries its quotes, and the service answers `Invalid table name`
   * for it, so the value has to be unquoted here.
   */
  private static String unquoteShowStmtIdentifier(ShowStmtIdentifierContext ctx) {
    String text = ctx.getText();
    if (ctx.simpleStringLiteral() == null) {
      return unquoteIdentifier(text);
    }
    String unquoted = unquoteStringLiteral(text);
    if (unquoted.isEmpty()) {
      // An empty literal is kept as it was written on purpose. The list tables request reads an
      // empty `name` parameter as "no filter at all" and answers with every table in the project,
      // so normalizing '' to an empty value would turn an empty prefix into a full listing.
      // Answering "no rows" locally instead, to match how plain SQL treats the same statement,
      // would skip the request altogether and make a missing project or schema look like an
      // empty namespace, so that is not the fix either. Passing the literal through keeps the
      // rejection the service already returns for it.
      return text;
    }
    return unquoted;
  }

  /**
   * Remove the enclosing quotes of a string literal and resolve the escape sequences the lexer
   * allows. Unknown escapes are kept as they are, they are not ours to interpret.
   */
  private static String unquoteStringLiteral(String literal) {
    if (literal.length() < 2) {
      return literal;
    }
    char quote = literal.charAt(0);
    if ((quote != '\'' && quote != '"')
        || literal.charAt(literal.length() - 1) != quote) {
      return literal;
    }
    String content = literal.substring(1, literal.length() - 1);
    StringBuilder unquoted = new StringBuilder(content.length());
    for (int i = 0; i < content.length(); i++) {
      char current = content.charAt(i);
      if (current != '\\' || i + 1 >= content.length()) {
        unquoted.append(current);
        continue;
      }
      char escaped = content.charAt(++i);
      switch (escaped) {
        case '\\':
        case '\'':
        case '"':
          unquoted.append(escaped);
          break;
        case 'n':
          unquoted.append('\n');
          break;
        case 't':
          unquoted.append('\t');
          break;
        case 'r':
          unquoted.append('\r');
          break;
        default:
          unquoted.append(current).append(escaped);
          break;
      }
    }
    return unquoted.toString();
  }

  /** Turn a quoted identifier `name` into name, `` inside it stands for a single back quote. */
  private static String unquoteIdentifier(String identifier) {
    if (identifier.length() < 2 || identifier.charAt(0) != '`'
        || identifier.charAt(identifier.length() - 1) != '`') {
      return identifier;
    }
    return identifier.substring(1, identifier.length() - 1).replace("``", "`");
  }

  public Command getCommand() {
    return command;
  }
}
