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

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.sqa.commandapi.utils.CommandUtil;

/**
 * 与 ShowTablesCommandTest 的分工: 那个类要在真实服务上跑, 这里只锁语法解析侧的取值, 无网络依赖。
 *
 * `show tables like` 的操作数在语法里是 showStmtIdentifier, 裸标识符和带引号的字符串字面量都合法,
 * 而它最终是 list tables 请求的 name 参数(表名前缀)。带引号的原文直接送出去服务端会报
 * Invalid table name, 所以解析侧必须还原成引号里的值; 唯一例外是空字面量, 见下面两例。
 */
public class ShowTablesLikeQuotingTest {

  private static ShowTablesCommand parse(String commandText) {
    Command command = CommandUtil.parseCommand(commandText);
    Assert.assertTrue("should be parsed as a command: " + commandText,
                      command instanceof ShowTablesCommand);
    return (ShowTablesCommand) command;
  }

  @Test
  public void singleQuotedPrefixIsUnquoted() {
    Assert.assertEquals("zz_probe_tbl", parse("show tables like 'zz_probe_tbl';").getPrefix());
    Assert.assertEquals("zz_probe_tbl", parse("show tables like 'zz_probe_tbl' ;").getPrefix());
  }

  @Test
  public void doubleQuotedPrefixIsUnquoted() {
    Assert.assertEquals("zz_probe_tbl", parse("show tables like \"zz_probe_tbl\";").getPrefix());
  }

  @Test
  public void quotedIdentifierIsUnquoted() {
    Assert.assertEquals("zz_probe_tbl", parse("show tables like `zz_probe_tbl`;").getPrefix());
    Assert.assertEquals("a`b", parse("show tables like `a``b`;").getPrefix());
  }

  @Test
  public void bareIdentifierIsKeptAsIs() {
    Assert.assertEquals("zz_probe_tbl", parse("show tables like zz_probe_tbl;").getPrefix());
    Assert.assertNull(parse("show tables;").getPrefix());
  }

  @Test
  public void escapeSequencesInsideLiteralAreResolved() {
    // 反引号用 char 拼出来, 免得转义层数在源码里数错
    String bs = String.valueOf((char) 92);
    // 'a\'b' 里的 \' 是一个引号字符
    Assert.assertEquals("a'b", parse("show tables like 'a" + bs + "'b';").getPrefix());
    // 'a\"b' 里的 \" 是一个双引号字符
    Assert.assertEquals("a\"b", parse("show tables like 'a" + bs + "\"b';").getPrefix());
    // 'a\\b' 里的 \\ 是一个反斜杠字符
    Assert.assertEquals("a" + bs + "b", parse("show tables like 'a" + bs + bs + "b';").getPrefix());
  }

  @Test
  public void wildcardCharacterIsNotConsumedByUnquoting() {
    // 通配是否支持是另一个问题, 这里只锁剥引号不会顺手改掉字面量本身
    Assert.assertEquals("sale*", parse("show tables like 'sale*';").getPrefix());
    Assert.assertEquals("sale%", parse("show tables like 'sale%';").getPrefix());
  }

  @Test
  public void projectAndSchemaAreUnquoted() {
    ShowTablesCommand command = parse("show tables in `my project`.`my schema` like 'tbl';");
    Assert.assertEquals("my project", command.getProject());
    Assert.assertEquals("my schema", command.getSchema());
    Assert.assertEquals("tbl", command.getPrefix());

    Assert.assertEquals("my_project",
                        parse("show tables in my_project like 'tbl';").getProject());
  }

  /**
   * 空前缀不能退化成"没有过滤条件"。
   *
   * list tables 请求把空的 name 参数读成"不过滤", 会返回整个项目的表, 连不存在的项目或
   * schema 都会被读成"没有表"。所以空字面量在这里保持原样(引号一起交给服务端),
   * 服务端按它今天的形态拒绝, SDK 不替它改变含义。
   */
  @Test
  public void emptyQuotedPrefixIsNotTurnedIntoNoFilter() {
    Assert.assertEquals("''", parse("show tables like '';").getPrefix());
    Assert.assertEquals("\"\"", parse("show tables like \"\";").getPrefix());
  }

  @Test
  public void onlyTheEmptyLiteralKeepsItsQuotes() {
    // 非空值照旧剥引号, 空前缀的例外不能溢出到别的长度
    Assert.assertEquals("s", parse("show tables like 's';").getPrefix());
    Assert.assertEquals("sale_a", parse("show tables like 'sale_a';").getPrefix());
  }
}

