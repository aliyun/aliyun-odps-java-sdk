package com.aliyun.odps.account;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;

/**
 * Created by zhenhong.gzh on 16/7/22.
 */
public class BearerTokenAccountTest extends TestBase {
  private static String tableName = OdpsTestUtils.getRandomTableName();

  private static TableSchema schema = new TableSchema();

  private void createTable() throws OdpsException {
    if (!odps.tables().exists(tableName)) {
      schema.addColumn(new Column("c1", OdpsType.BIGINT));
      schema.addColumn(new Column("c2", OdpsType.BOOLEAN));
      schema.addColumn(new Column("c3", OdpsType.DATETIME));
      schema.addColumn(new Column("c4", OdpsType.STRING));

      tableName = OdpsTestUtils.getRandomTableName();
      odps.tables().create(tableName, schema);
    }
  }

  @Test
  public void testPositive() throws OdpsException {
    createTable();

    String sql = String.format("select count(*) from %s;", tableName);
    String taskName = "test_sql";
    SQLTask task = new SQLTask();
    task.setQuery(sql);
    task.setName(taskName);
    Instance instance = odps.instances().create(task);
    instance.waitForSuccess();

    String token = odps.logview().generateInstanceToken(instance, 7*24);

    Odps bearerTokenOdps = new Odps(new BearerTokenAccount(token));
    bearerTokenOdps.setDefaultProject(odps.getDefaultProject());
    bearerTokenOdps.setEndpoint(odps.getEndpoint());
    Instance bearRequestIns = bearerTokenOdps.instances().get(instance.getId());

    Assert.assertEquals(instance.getTaskSummary(taskName), bearRequestIns.getTaskSummary(taskName));

    Assert.assertEquals(instance.getTaskResults().get(taskName),
                        bearRequestIns.getTaskResults().get(taskName));

    odps.tables().delete(taskName, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegative() throws OdpsException {
    Account account = new BearerTokenAccount(null);
    Odps tokenOdps = new Odps(odps);
    tokenOdps.setAccount(account);

    tokenOdps.tables().create("test_null", schema);
  }

  @Test(expected = OdpsException.class)
  public void testErrorToken() throws OdpsException {
    Account account = new BearerTokenAccount("test_error");
    Odps tokenOdps = new Odps(account);
    tokenOdps.setDefaultProject(odps.getDefaultProject());
    tokenOdps.setEndpoint(odps.getEndpoint());

    tokenOdps.tables().create(OdpsTestUtils.getRandomTableName(), schema);
  }
}
