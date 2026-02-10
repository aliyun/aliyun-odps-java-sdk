package com.aliyun.odps.data;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.sqa.ExecuteMode;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.sqa.SQLExecutorBuilder;
import com.aliyun.odps.sqa.MaxQATest;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class InstanceDataIteratorTest {

  private static Odps odps;
  private static final String TABLE_NAME = "instance_data_iterator_test_table";
  private static Instance instance;

  @BeforeClass
  public static void initTable() throws Exception {
    odps = OdpsTestUtils.newDefaultOdps();
    odps.tables().delete(TABLE_NAME, true);
    odps.tables().newTableCreator(TABLE_NAME, TableSchema.builder().withBigintColumn("c1").build())
        .ifNotExists().withLifeCycle(1L).create();
    TableTunnel.UploadSession
        uploadSession =
        odps.tableTunnel().createUploadSession(odps.getDefaultProject(), TABLE_NAME);
    Record record = uploadSession.newRecord();
    RecordWriter recordWriter = uploadSession.openRecordWriter(0L);
    for (long i = 0; i < 10000; i++) {
      record.set(0, i);
      recordWriter.write(record);
    }
    recordWriter.close();
    uploadSession.commit(new Long[]{0L});

    instance = SQLTask.run(odps, "select * from " + TABLE_NAME + ";");
    instance.waitForSuccess();
  }

  @Test
  public void testFullRead() throws TunnelException {
    InstanceDataIterator
        instanceDataIterator =
        new InstanceDataIterator(odps, instance, 0, -1, 500, 3, -1);
    long count = 0;
    while (instanceDataIterator.hasNext()) {
      Record record = instanceDataIterator.next();
      Assert.assertEquals(count, record.get(0));
      count++;
    }
    Assert.assertEquals(10000, count);
  }

  @Test
  public void testOffsetAndReadCount() throws TunnelException {
    InstanceDataIterator
        instanceDataIterator =
        new InstanceDataIterator(odps, instance, 25, 2000, 500, 3, -1);
    long offset = 25;
    while (instanceDataIterator.hasNext()) {
      Record record = instanceDataIterator.next();
      Assert.assertEquals(offset, record.get(0));
      offset++;
    }
    Assert.assertEquals(2000 + 25, offset);
    Assert.assertEquals(2000, instanceDataIterator.getRecordCount());
  }

  @Test
  public void testOnlyOffset() throws TunnelException {
    InstanceDataIterator
        instanceDataIterator =
        new InstanceDataIterator(odps, instance, 25, -1, 500, 3, -1);
    long offset = 25;
    while (instanceDataIterator.hasNext()) {
      Record record = instanceDataIterator.next();
      Assert.assertEquals(offset, record.get(0));
      offset++;
    }
    Assert.assertEquals(10000, offset);
    Assert.assertEquals(10000 - 25, instanceDataIterator.getRecordCount());
  }

  @Test
  public void testOfflineTest() throws Exception {
    SQLExecutor executor = SQLExecutorBuilder.builder()
        .odps(odps)
        .executeMode(ExecuteMode.OFFLINE)
        .fetchResultSplitSize(500)
        .fetchResultPreloadSplitNum(10)
        .fetchResultThreadNum(3)
        .build();
    executor.run("select * from " + TABLE_NAME + ";", null);

    ResultSet resultSet = executor.getResultSet();
    long count = 0;
    while (resultSet.hasNext()) {
      Record record = resultSet.next();
      Assert.assertEquals(count, record.get(0));
      count++;
    }
    Assert.assertEquals(10000, count);
    Assert.assertEquals(10000, resultSet.getRecordCount());
  }

  @Test
  public void testMcqaV1Test() throws Exception {
    SQLExecutor executor = SQLExecutorBuilder.builder()
        .odps(odps)
        .executeMode(ExecuteMode.INTERACTIVE)
        .fetchResultSplitSize(500)
        .fetchResultPreloadSplitNum(10)
        .fetchResultThreadNum(3)
        .build();
    executor.run("select * from " + TABLE_NAME + ";", null);

    System.out.println(executor.getLogView());

    // not use instance data iterator
    ResultSet resultSet = executor.getResultSet();
    long count = 0;
    while (resultSet.hasNext()) {
      Record record = resultSet.next();
      Assert.assertEquals(count, record.get(0));
      count++;
    }
    Assert.assertEquals(10000, count);
  }

  @Test
  public void testMcqaV2Test() throws Exception {
    Assume.assumeTrue(odps.projects().exists(MaxQATest.PROJECT_NAME));
    Assume.assumeTrue(odps.tables().exists(MaxQATest.PROJECT_NAME, TABLE_NAME));

    Odps clone = odps.clone();
    clone.setDefaultProject(MaxQATest.PROJECT_NAME);
    SQLExecutor executor = SQLExecutorBuilder.builder()
        .odps(clone)
        .enableMaxQA(true)
        .quotaName(MaxQATest.QUOTA_NAME)
        .fetchResultSplitSize(500)
        .fetchResultPreloadSplitNum(10)
        .fetchResultThreadNum(3)
        .build();
    executor.run("select * from " + TABLE_NAME + ";", null);

    ResultSet resultSet = executor.getResultSet();
    long count = 0;
    while (resultSet.hasNext()) {
      Record record = resultSet.next();
      Assert.assertEquals(count, record.get(0));
      count++;
    }
    Assert.assertEquals(10000, count);
    Assert.assertEquals(10000, resultSet.getRecordCount());
  }
}
