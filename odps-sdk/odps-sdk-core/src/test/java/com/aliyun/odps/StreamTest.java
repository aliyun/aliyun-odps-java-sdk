package com.aliyun.odps;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.table.StreamIdentifier;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class StreamTest {

  private static Odps odps;

  private static String tableName = "stream_object_test_table";
  private static String streamName = "stream_object_test";

  @BeforeClass
  public static void init() throws OdpsException {
    odps = OdpsTestUtils.newSchemaOdps();
    Assume.assumeTrue(odps.projects().exists(odps.getDefaultProject()));
    TableSchema schema = new TableSchema();
    Column pk = Column.newBuilder("key1", TypeInfoFactory.BIGINT).notNull().build();
    Column pk2 = Column.newBuilder("key2", TypeInfoFactory.BIGINT).notNull().build();
    schema.addColumn(pk);
    schema.addColumn(pk2);
    schema.addColumn(new Column("c2", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("c3", TypeInfoFactory.BIGINT));
    odps.tables().delete(tableName, true);
    odps.tables().newTableCreator(odps.getDefaultProject(), tableName, schema)
        .withPrimaryKeys(ImmutableList.of("key1", "key2"))
        .withSchemaName(odps.getCurrentSchema()).transactionTable().ifNotExists().debug()
        .create();
    testCreateStream();
  }

  public static void testCreateStream() throws OdpsException {
    odps.streams().delete(streamName, true);
    odps.streams().create(
        StreamIdentifier.of(odps.getDefaultProject(), streamName),
        TableIdentifier.of(odps.getDefaultProject(), odps.getCurrentSchema(), tableName));
  }

  @Test
  public void testStreamExists() throws OdpsException {
    boolean exists = odps.streams().exists(streamName);
    Assert.assertTrue(exists);
  }

  @Test
  public void testListStreams() {
    Iterator<Stream> iterator = odps.streams().iterator();
    int num = 0;
    while (iterator.hasNext()) {
      num++;
      Stream next = iterator.next();
      System.out.println(next.getName());
    }
    Assert.assertTrue(num > 0);
  }

  @Test
  public void testGetStreams() throws OdpsException {
    Stream stream = odps.streams().get(streamName);
    stream.reload();
    Assert.assertEquals(tableName, stream.getRefTableName());
    Assert.assertEquals(odps.getDefaultProject(), stream.getRefTableProject());
    System.out.println(stream.getCreateTime());
  }
}
