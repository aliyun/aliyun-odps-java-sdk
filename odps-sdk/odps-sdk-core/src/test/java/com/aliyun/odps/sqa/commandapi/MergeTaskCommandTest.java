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

import com.aliyun.odps.TestBase;

public class MergeTaskCommandTest extends TestBase {

  /**
   * 语法测试
   */
  @Test
  public void parseMergeTaskCommandTest() {
    String[] positiveCommands = {
        "alter table name archive;",
        "alter\t\r\n table tbl_name  partition (dt='s') compact minor;",
        "alter table a11 partition (ab = '123') freeze;",
        "alter table a1 partition (part ='ab') merge  smallfiles; ",
        "alter table table.subtable partition (dt= 's') restore  ;",
        "alter table table.subtable.thirdtable partition (dt= 's') restore  ;"
    };
    Assert.assertEquals(0, CommandTestUtil.getErrorNum(positiveCommands));

    String[] negativeCommands = {
        "alter tabletable name archive;",
        "alter\t\r\n table tbl_name  partition (dt='s' compact minor;",
        "alter table 11 partition (ab = 123) freeze;",
        "alter table a1 partition(123 ='ab') merge  ; ",
        "alter table a1 partition(123 ='ab') merge smallfiles; ",
        "alter table a1 partition ('ab') merge smallfiles; ",
        "alter table table.subtable (dt= 's') restore  ;"
    };
    Assert.assertEquals(negativeCommands.length, CommandTestUtil.getErrorNum(negativeCommands));
  }

}