package com.aliyun.odps;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.task.SQLTask;

public class StorageTierTest extends TestBase {

  private static Map<String, String> hint;
  private static Odps odps;
  @BeforeClass
  public static void initOdpsFlag() {
    hint = new HashMap<>();
    hint.put("odps.tiered.storage.enable", "true");
    odps = OdpsTestUtils.newStorageTierOdps();
  }

  @Test
  public void testCreateTable() throws OdpsException {
    // 测试创建表时指定存储类型
    String name = OdpsTestUtils.getRandomName();
    BiFunction<String, String, String>
        sql =
        (tableName, storageTier) -> ("create table if not exists " + tableName
                                     + " (name string,id bigint) TBLPROPERTIES ('storagetier'='"
                                     + storageTier + "');");
    boolean ok = true;
    final String[] tierParameter = {"lowfrequency", "longterm", "standard"};
    for (String tier : tierParameter) {
      try {
        Instance
            ins =
            SQLTask.run(odps, odps.getDefaultProject(), sql.apply(name, tier), hint, null);
        ins.waitForSuccess();
        // 初始化创建是standard
        Table table = odps.tables().get(name);
        // 测试实际的环境
        table.reloadExtendInfo();
        assertNotNull(table.getStorageTierInfo().getStorageTier());
        assertEquals(tier,
                     table.getStorageTierInfo().getStorageTier().toString().toLowerCase());
        assertNotNull(table.getStorageTierInfo().getStorageLastModifiedTime());
        System.out.println(
            "LastModifiedTime :" + table.getStorageTierInfo().getStorageLastModifiedTime());
      } catch (Exception e) {
        ok = false;
        System.out.println("something error in testCreateTable " + e.getMessage());
      } finally {
        odps.tables().delete(name);
        System.out.println("delete table ok!");
        if (!ok) {
          fail();
        }
      }
    }
  }

  @Test
  public void testTableStorageTier() throws OdpsException {
    String name = OdpsTestUtils.getRandomName();
    //创建一个非分区表，设置其存储类型
    boolean ok = true;
    String sql = "create table if not exists " + name + " (name string,id bigint);";
    try {
      Instance ins = SQLTask.run(odps, odps.getDefaultProject(), sql, hint, null);
      ins.waitForSuccess();
      // 初始化创建是standard
      Table table = odps.tables().get(name);
      // 打标测试 三种类型
      setTableStorageTier("standard", table.getName());
      assertEquals("standard",
                   table.getStorageTierInfo().getStorageTier().toString().toLowerCase());
      setTableStorageTier("lowfrequency", table.getName());
      table.reloadExtendInfo();
      assertEquals("lowfrequency",
                   table.getStorageTierInfo().getStorageTier().toString().toLowerCase());
      setTableStorageTier("longterm", table.getName());
      table.reloadExtendInfo();
      assertEquals("longterm",
                   table.getStorageTierInfo().getStorageTier().toString().toLowerCase());

      // 使用反射来进行测试mock的数据类型 lastModifiedTime(), StorageTier 类型
      Method lazyLoadExtendInfo = Table.class.getDeclaredMethod("loadSchemaFromJson", String.class);
      lazyLoadExtendInfo.setAccessible(true);
      String mockData = readToString("table-storagetier.json");
      lazyLoadExtendInfo.invoke(table, mockData);
      assertEquals("standard",
                   table.getStorageTierInfo().getStorageTier().toString().toLowerCase());
      System.out.println(table.getStorageTierInfo().getStorageLastModifiedTime());
      assertNotNull(table.getStorageTierInfo().getStorageLastModifiedTime());
      // 测试实际的环境
      table.reloadExtendInfo();
      assertNotNull(table.getStorageTierInfo().getStorageTier());
      System.out.println("storageTier: " + table.getStorageTierInfo().getStorageTier());
      assertNotNull(table.getStorageTierInfo().getStorageLastModifiedTime());
      System.out.println(
          "LastModifiedTime :" + table.getStorageTierInfo().getStorageLastModifiedTime());

    } catch (Exception e) {
      ok = false;
      System.out.println("something error in testTableStorageTier " + e.getMessage());
    } finally {
      odps.tables().delete(name);
      System.out.println("delete table ok!");
      if (!ok) {
        fail();
      }
    }

  }

  private void setTableStorageTier(String storageTier, String table) throws OdpsException {
    if (StorageTierInfo.StorageTier.getStorageTierByName(storageTier) == null) {
      throw new RuntimeException("Invalid StorageTier type!");
    }
    //ALTER TABLE <TABLE_NAME> SET TBLPROPERTIES("storagetier"="standard|lowfrequency|longterm");
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ").append(table);
    sb.append(" SET TBLPROPERTIES('storagetier'='").append(storageTier);
    sb.append("');");
    System.out.println("sql:" + sb);
    // new SQLTask
    Instance i = SQLTask.run(odps, odps.getDefaultProject(), sb.toString(), hint, null);
    i.waitForSuccess();
  }

  @Test
  public void testPartitionStorageTier() throws OdpsException {
    String name = OdpsTestUtils.getRandomName();
    boolean ok = true;
    try {
      //创建一个非分区表，设置其存储类型
      String
          sql =
          String.format(
              "create table if not exists %s "
              + "(name string,id bigint) "
              + "partitioned by(sale_date string,region string);",
              name);
      Instance ins = SQLTask.run(odps, odps.getDefaultProject(), sql, hint, null);
      ins.waitForSuccess();
      if (!ins.isSuccessful()) {
        fail();
      }
      sql = String.format("alter table %s add partition (sale_date='2023', region='china');", name);
      // 初始化创建是standard
      ins = SQLTask.run(odps, odps.getDefaultProject(), sql, hint, null);
      ins.waitForSuccess();
      if (!ins.isSuccessful()) {
        fail();
      }
      System.out.println("add partition ok!");
      Table table = odps.tables().get(name);
      //assertEquals("standard", table.getStorageTierType().toString().toLowerCase());
      PartitionSpec partitionSpec = new PartitionSpec("sale_date='2023',region='china'");
      Partition partition = table.getPartition(partitionSpec);
      // 分区表打标测试 三种存储类型
      setPartitionStorageTier("lowfrequency", partitionSpec, table.getName());
      partition.reloadExtendInfo();
      assertEquals("lowfrequency",
                   partition.getStorageTierInfo().getStorageTier().toString().toLowerCase());
      setPartitionStorageTier("longterm", partitionSpec, table.getName());
      partition.reloadExtendInfo();
      assertEquals("longterm",
                   partition.getStorageTierInfo().getStorageTier().toString().toLowerCase());
      setPartitionStorageTier("standard", partitionSpec, table.getName());
      partition.reloadExtendInfo();
      assertEquals("standard",
                   partition.getStorageTierInfo().getStorageTier().toString().toLowerCase());
      // 分区表 mock 数据测试 分区大小
      String mockData = readToString("partition-storagetier.json");
      Method lazyLoadExtendInfo = Table.class.getDeclaredMethod("loadSchemaFromJson", String.class);
      lazyLoadExtendInfo.setAccessible(true);
      lazyLoadExtendInfo.invoke(table, mockData);
      StorageTierInfo storageTierInfo = table.getStorageTierInfo();
      assertEquals(Long.valueOf(1024L), storageTierInfo.getStorageSize("Standard"));
      assertEquals(Long.valueOf(2048L), storageTierInfo.getStorageSize("LowFrequency"));
      assertEquals(Long.valueOf(1024L), storageTierInfo.getStorageSize("LongTerm"));

      table.reloadExtendInfo();
      // 测试真实场景
      assertNotNull(table.getStorageTierInfo().getStorageSize("Standard"));
      System.out.println(
          "StandardSize :" + table.getStorageTierInfo().getStorageSize("Standard"));
      assertNotNull(table.getStorageTierInfo().getStorageSize("LowFrequency"));
      System.out.println(
          "LowFrequencySize :" + table.getStorageTierInfo().getStorageSize("LowFrequency"));
      assertNotNull(table.getStorageTierInfo().getStorageSize("LongTerm"));
      System.out.println("LongTerm :" + table.getStorageTierInfo().getStorageSize("LongTerm"));

    } catch (Exception e) {
      ok = false;
      System.out.println("something error in testPartitionStorageTier " + e.getMessage());
    } finally {
      System.out.println("delete table ok");
      odps.tables().delete(name);
      if (!ok) {
        fail();
      }
    }
  }

  /**
   * 对特定分区表进行存储类型打标
   *
   * @param storageTier
   * @param spec
   */
  private void setPartitionStorageTier(String storageTier, PartitionSpec spec, String table)
      throws OdpsException {
    if (StorageTierInfo.StorageTier.getStorageTierByName(storageTier) == null) {
      throw new RuntimeException("Invalid StorageTier type, please check your input");
    }
    //ALTER TABLE <TABLE_NAME> [PARTITION_SPEC]
    //SET PARTITIONPROPERTIES ("storagetier"="standard|lowfrequency|longterm");
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER TABLE ").append(table);
    sb.append(" PARTITION(");

    String[] keys = spec.keys().toArray(new String[0]);
    for (int i = 0; i < keys.length; i++) {
      sb.append(keys[i]).append("='").append(spec.get(keys[i])).append("'");
      if (i + 1 < keys.length) {
        sb.append(',');
      }
    }
    sb.append(") SET PARTITIONPROPERTIES('storagetier'='").append(storageTier);
    sb.append("');");
    System.out.println("sql:" + sb);

    // new SQLTask
    Instance i = SQLTask.run(odps, odps.getDefaultProject(), sb.toString(), hint, null);
    i.waitForSuccess();
  }

  @Test
  public void testProjectStorageTier() {
    boolean ok = true;
    try {
      // 读取xml,查看是否存在xml
      byte[] xml = readToByteArray("project-storage-tier.xml");
      Project.ProjectModel model = SimpleXmlUtils.unmarshal(xml, Project.ProjectModel.class);
      if (model.extendedProperties != null) {
        StorageTierInfo
            storageTierInfo =
            StorageTierInfo.getStorageTierInfo(model.extendedProperties);
        String[] types = {"standard", "LowFrequency", "LongTerm"};
        Long[] expects = {1024L, 2048L, 1024L};
        for (int i = 0; i < 3; i++) {
          String type = types[i];
          Long expect = expects[i];
          Long value = storageTierInfo.getStorageSize(type);
          assertEquals(expect, value);
        }
      }

    } catch (Exception e) {
      ok = false;
      System.out.println("something error in testProjectStorageTier " + e.getMessage());
    } finally {
      if (!ok) {
        fail();
      }
    }
  }

  private static String readToString(String path) {
    InputStream inputStream = StorageTierTest.class.getClassLoader().getResourceAsStream(path);
    assert inputStream != null;
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    StringBuilder stringBuilder = new StringBuilder();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return stringBuilder.toString();
  }

  private static byte[] readToByteArray(String xmlPath) throws UnsupportedEncodingException {
    return readToString(xmlPath).getBytes("UTF-8");
  }
}

