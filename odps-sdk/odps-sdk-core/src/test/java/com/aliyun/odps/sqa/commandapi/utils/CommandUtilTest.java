package com.aliyun.odps.sqa.commandapi.utils;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.TestBase;

/**
 * 三层模型的逻辑测试
 */
public class CommandUtilTest extends TestBase {

  @Test
  public void schemaNameTest() {
    String projectName;
    String schemaName;
    String tableName;

    // p.s.t
    projectName = "myproject";
    schemaName = "myschema";
    tableName = "mytable";

    String schemaCopy = schemaName;
    Assert.assertEquals("myschema",
                        CommandUtil.getRealSchemaName(odps, projectName, schemaName, false));
    Assert.assertEquals("myproject",
                        CommandUtil.getRealProjectName(odps, projectName, schemaCopy, false));
    Assert.assertEquals("myschema",
                        CommandUtil.getRealSchemaName(odps, projectName, schemaName, true));
    Assert.assertEquals("myproject",
                        CommandUtil.getRealProjectName(odps, projectName, schemaCopy, true));

    // p.t
    schemaName = null;
    schemaCopy = schemaName;
    Assert.assertEquals(null, CommandUtil.getRealSchemaName(odps, projectName, schemaName, false));
    Assert.assertEquals("myproject",
                        CommandUtil.getRealProjectName(odps, projectName, schemaCopy, false));
    Assert.assertEquals("myproject",
                        CommandUtil.getRealSchemaName(odps, projectName, schemaName, true));
    Assert.assertEquals(odps.getDefaultProject(),
                        CommandUtil.getRealProjectName(odps, projectName, schemaCopy, true));

    // s.t
    // 不需要测试，因为语法树只能解析为p.t。替换过程发生在command.run逻辑里

    // t
    projectName = null;
    Assert.assertEquals(null, CommandUtil.getRealSchemaName(odps, projectName, schemaName, false));
    Assert.assertEquals(odps.getDefaultProject(),
                        CommandUtil.getRealProjectName(odps, projectName, schemaCopy, false));
    Assert.assertEquals(odps.getCurrentSchema(),
                        CommandUtil.getRealSchemaName(odps, projectName, schemaName, true));
    Assert.assertEquals(odps.getDefaultProject(),
                        CommandUtil.getRealProjectName(odps, projectName, schemaCopy, true));

  }

}
