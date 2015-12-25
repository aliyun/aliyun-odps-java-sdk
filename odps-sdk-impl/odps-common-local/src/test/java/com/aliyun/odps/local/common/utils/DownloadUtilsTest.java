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

package com.aliyun.odps.local.common.utils;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.ArchiveResource;
import com.aliyun.odps.Column;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.PyResource;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Resource.Type;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.WareHouse;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;

public class DownloadUtilsTest {

  static String project;
  static String partTable = DownloadUtils.class.getSimpleName() + "_download_part_table";
  static String nonePartTable = DownloadUtils.class.getSimpleName() + "_download_none_part_table";
  static String fileResource = DownloadUtils.class.getSimpleName() + "_download_file_resource.tar";
  static String partTableResource = DownloadUtils.class.getSimpleName() + "_download_part_table_resource1";
  static String nonePartTableResource = DownloadUtils.class.getSimpleName() + "_download_none_part_table_resource1";
  static Odps odps;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    odps = TestUtils.newOdps();
    project = odps.getDefaultProject();
    WareHouse.getInstance().setOdps(odps);
    init();
  }

  public static void init() throws OdpsException, IOException {

    if (!odps.tables().exists(partTable)) {
      TableSchema schema = new TableSchema();
      schema.addColumn(new Column("c1", OdpsType.STRING));
      schema.addColumn(new Column("c2", OdpsType.BIGINT));
      schema.addColumn(new Column("c3", OdpsType.DOUBLE));
      schema.addColumn(new Column("c4", OdpsType.BOOLEAN));
      schema.addColumn(new Column("c5", OdpsType.DATETIME));
      schema.addPartitionColumn(new Column("p1", OdpsType.STRING));
      schema.addPartitionColumn(new Column("p2", OdpsType.STRING));
      odps.tables().create(partTable, schema);
      odps.tables().get(partTable).createPartition(new PartitionSpec("p1='1',p2='1'"));
      odps.tables().get(partTable).createPartition(new PartitionSpec("p1='1',p2='2'"));

      TableTunnel tunnel = TestUtils.newTableTunnel(odps);
      TableTunnel.UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(),
                                                                     partTable, new PartitionSpec(
          "p1='1',p2='1'"));
      TunnelRecordWriter rw = (TunnelRecordWriter) session.openRecordWriter(0L);
      Record record = session.newRecord();
      record.set(0, "col1");
      record.set(1, 1L);
      record.set(2, 1.1D);
      record.set(3, true);
      record.set(4, new Date());
      rw.write(record);
      rw.close();
      Long[] blocks = {0L};
      session.commit(blocks);

      session = tunnel.createUploadSession(odps.getDefaultProject(), partTable, new PartitionSpec(
          "p1='1',p2='2'"));
      rw = (TunnelRecordWriter) session.openRecordWriter(0L);
      record = session.newRecord();
      record.set(0, "col2");
      record.set(1, 2L);
      record.set(2, 2.2D);
      record.set(3, false);
      record.set(4, new Date());
      rw.write(record);
      rw.close();
      blocks[0] = 0L;
      session.commit(blocks);

    }

    if (!odps.tables().exists(nonePartTable)) {
      TableSchema schema = new TableSchema();
      schema.addColumn(new Column("c1", OdpsType.STRING));
      schema.addColumn(new Column("c2", OdpsType.BIGINT));
      schema.addColumn(new Column("c3", OdpsType.DOUBLE));
      schema.addColumn(new Column("c4", OdpsType.BOOLEAN));
      schema.addColumn(new Column("c5", OdpsType.DATETIME));
      odps.tables().create(nonePartTable, schema);
      TableTunnel tunnel = TestUtils.newTableTunnel(odps);
      TableTunnel.UploadSession session = tunnel.createUploadSession(odps.getDefaultProject(),
                                                                     nonePartTable);
      TunnelRecordWriter rw = (TunnelRecordWriter) session.openRecordWriter(0L);
      Record record = session.newRecord();
      record.set(0, "col2");
      record.set(1, 2L);
      record.set(2, 2.2D);
      record.set(3, false);
      record.set(4, new Date());
      rw.write(record);
      rw.close();
      Long[] blocks = {0L};
      session.commit(blocks);
    }

    if (!odps.resources().exists(partTableResource)) {
      TableResource rm = new TableResource(partTable, odps.getDefaultProject(), new PartitionSpec(
          "p1='1',p2='2'"));
      rm.setName(partTableResource);
      odps.resources().create(rm);
    }

    if (!odps.resources().exists(nonePartTableResource)) {
      TableResource rm = new TableResource(nonePartTable, odps.getDefaultProject());
      rm.setName(nonePartTableResource);
      odps.resources().create(rm);
    }

  }

  // @Test
  public void testDownloadTable() throws OdpsException, IOException {
    // //////Test table////////
    WareHouse.getInstance().dropTableIfExists(project, nonePartTable);
    Assert.assertEquals(false, WareHouse.getInstance().existsTable(project, nonePartTable));
    TableInfo tableInfo = TableInfo.builder().projectName(project).tableName(nonePartTable).build();
    DownloadUtils.downloadTableSchemeAndData(odps, tableInfo, 10, ',');
    List<Object[]> dataList = WareHouse.getInstance().readData(project, nonePartTable, null, null,
                                                               ',');
    Assert.assertNotNull(dataList);
    Assert.assertEquals(1, dataList.size());
    Object[] record = dataList.get(0);
    // col2,2,2.2,false,2014-01-01 01:02:03 000
    Assert.assertEquals(true, record[0] instanceof String);
    Assert.assertEquals("col2", (String) record[0]);
    Assert.assertEquals(true, record[1] instanceof Long);
    Assert.assertEquals(2L, ((Long) record[1]).longValue());
    Assert.assertEquals(true, record[2] instanceof Double);
    Assert.assertEquals(true, ((Double) record[2]).doubleValue() == 2.2D);
    Assert.assertEquals(true, record[3] instanceof Boolean);
    Assert.assertEquals(false, (Boolean) record[3]);
    Assert.assertEquals(true, record[4] instanceof Date);

    // //////Test Partition table////////
    WareHouse.getInstance().dropTableIfExists(project, partTable);
    Assert.assertEquals(false, WareHouse.getInstance().existsTable(project, partTable));
    tableInfo = TableInfo.builder().projectName(project).tableName(partTable)
        .partSpec(new PartitionSpec("p1=1,p2=2")).build();
    DownloadUtils.downloadTableSchemeAndData(odps, tableInfo, 10, ',');
    dataList = WareHouse.getInstance().readData(project, partTable, new PartitionSpec("p1=1,p2=2"),
                                                null, ',');
    Assert.assertNotNull(dataList);
    Assert.assertEquals(1, dataList.size());
    record = dataList.get(0);
    // col2,2,2.2,false,2014-01-01 01:02:03 000
    Assert.assertEquals(true, record[0] instanceof String);
    Assert.assertEquals("col2", (String) record[0]);
    Assert.assertEquals(true, record[1] instanceof Long);
    Assert.assertEquals(2L, ((Long) record[1]).longValue());
    Assert.assertEquals(false, WareHouse.getInstance().existsResource(project, fileResource));
    Assert.assertEquals(true, record[2] instanceof Double);
    Assert.assertEquals(true, ((Double) record[2]).doubleValue() == 2.2D);
    Assert.assertEquals(true, record[3] instanceof Boolean);
    Assert.assertEquals(false, (Boolean) record[3]);
    Assert.assertEquals(true, record[4] instanceof Date);
  }

  @Test(expected = RuntimeException.class)
  public void testDownloadTableWithWrongPartSpec() throws IOException {
    WareHouse.getInstance().dropTableIfExists(project, partTable);
    Assert.assertEquals(false, WareHouse.getInstance().existsTable(project, partTable));
    TableInfo tableInfo = TableInfo.builder().projectName(project).tableName(partTable)
        .partSpec(new PartitionSpec("p1=2")).build();
    DownloadUtils.downloadTableSchemeAndData(odps, tableInfo, 10, ',');
  }

  @Test
  public void testDownloadResource() throws IOException {
    // ////////////test none partition table resource///////////
    WareHouse.getInstance().dropResourceIfExists(project, nonePartTableResource);
    WareHouse.getInstance().dropTableIfExists(project, nonePartTableResource);
    Assert.assertEquals(false,
                        WareHouse.getInstance().existsTable(project, nonePartTableResource));
    Assert.assertEquals(false,
                        WareHouse.getInstance().existsResource(project, nonePartTableResource));
    DownloadUtils.downloadResource(odps, project, nonePartTableResource, 10, ',');
    Assert.assertEquals(true, WareHouse.getInstance()
        .existsResource(project, nonePartTableResource));
    Assert.assertEquals(true,
                        WareHouse.getInstance().existsTable(project, nonePartTable));

    // ////////////test partition table resource///////////
    WareHouse.getInstance().dropResourceIfExists(project, partTableResource);
    WareHouse.getInstance().dropTableIfExists(project, partTableResource);
    Assert.assertEquals(false, WareHouse.getInstance().existsResource(project, partTableResource));
    Assert.assertEquals(false,
                        WareHouse.getInstance().existsTable(project, partTableResource));
    DownloadUtils.downloadResource(odps, project, partTableResource, 10, ',');
    Assert.assertEquals(true, WareHouse.getInstance().existsResource(project, partTableResource));
    Assert.assertEquals(true,
                        WareHouse.getInstance().existsTable(project, partTable));

  }

  @Test
  public void testResourceFile() throws IOException, OdpsException {
    String resourceName = "zhemin_res.file";
    WareHouse.getInstance().dropResourceIfExists(project, resourceName);
    Assert.assertEquals(false, WareHouse.getInstance().existsResource(project, resourceName));
    addResourceFile();
    DownloadUtils.downloadResource(odps, project, resourceName, 10, ',');
    Assert.assertEquals(true, WareHouse.getInstance().existsResource(project, resourceName));
    deleteResourceFile();
  }

  @Test
  public void testResourceJar() throws OdpsException, IOException {
    String resourceName = "zhemin_res.jar";
    WareHouse.getInstance().dropResourceIfExists(project, resourceName);
    Assert.assertEquals(false, WareHouse.getInstance().existsResource(project, resourceName));
    addResourceJar();
    DownloadUtils.downloadResource(odps, project, resourceName, 10, ',');
    Assert.assertEquals(true, WareHouse.getInstance().existsResource(project, resourceName));
    deleteResource(resourceName);
  }

  @Test
  public void testResourcePy() throws OdpsException, IOException {
    String resourceName = "zhemin_res.py";
    WareHouse.getInstance().dropResourceIfExists(project, resourceName);
    Assert.assertEquals(false, WareHouse.getInstance().existsResource(project, resourceName));
    addResourcePy();
    DownloadUtils.downloadResource(odps, project, resourceName, 10, ',');
    Assert.assertEquals(true, WareHouse.getInstance().existsResource(project, resourceName));
    deleteResource(resourceName);
  }

  @Test
  public void testResourceArchive() throws OdpsException, IOException {
    String resourceName = "zhemin_res.tar.gz";
    WareHouse.getInstance().dropResourceIfExists(project, resourceName);
    Assert.assertEquals(false, WareHouse.getInstance().existsResource(project, resourceName));
    addResourceArchive();
    DownloadUtils.downloadResource(odps, project, resourceName, 10, ',');
    Assert.assertEquals(true, WareHouse.getInstance().existsResource(project, resourceName));
    deleteResource(resourceName);
  }

  @Test
  public void testResourceTable() throws OdpsException, IOException {
    String resourceName = "zhemin_res_src";
    WareHouse.getInstance().dropResourceIfExists(project, resourceName);
    Assert.assertEquals(false, WareHouse.getInstance().existsResource(project, resourceName));
    addResourceTable();
    DownloadUtils.downloadResource(odps, project, resourceName, 10, ',');
    Assert.assertEquals(true, WareHouse.getInstance().existsResource(project, resourceName));
    deleteResource(resourceName);
  }

  @Test
  public void testResourceTablePartition() throws OdpsException, IOException {
    String resourceName = "zhemin_res_pt";
    WareHouse.getInstance().dropResourceIfExists(project, resourceName);
    Assert.assertEquals(false, WareHouse.getInstance().existsResource(project, resourceName));
    addResourceTablePartition();
    DownloadUtils.downloadResource(odps, project, resourceName, 10, ',');
    Assert.assertEquals(true, WareHouse.getInstance().existsResource(project, resourceName));
    deleteResource(resourceName);
  }

  private void addResourceTablePartition() throws OdpsException {
    String tableName = "table_for_test_abc";
    if (odps.tables().exists(tableName)) {
      odps.tables().delete(tableName);
    }
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    schema.addPartitionColumn(new Column("pt", OdpsType.STRING));
    odps.tables().create(tableName, schema);
    odps.tables().get(tableName).createPartition(new PartitionSpec("pt=1"));

    PartitionSpec pt = new PartitionSpec();
    pt.set("pt", "1");
    TableResource resource = new TableResource(tableName, odps.getDefaultProject(), pt);
    resource.setName("zhemin_res_pt");
    odps.resources().create(resource);
  }

  private void addResourceFile() throws FileNotFoundException, OdpsException {

    String filename = DownloadUtilsTest.class.getClassLoader().getResource("resource.txt")
        .getFile();
    FileResource rm = new FileResource();

    rm.setName("zhemin_res.file");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.file");
    assertEquals("type must be file", r.getType(), Type.FILE);
  }

  private void deleteResourceFile() throws OdpsException {
    odps.resources().delete("zhemin_res.file");
  }

  private void addResourceJar() throws FileNotFoundException, OdpsException {

    String filename = DownloadUtilsTest.class.getClassLoader().getResource("resource.jar")
        .getFile();
    JarResource rm = new JarResource();

    rm.setName("zhemin_res.jar");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.jar");
    assertEquals("type must be file", r.getType(), Type.JAR);
  }

  private void addResourcePy() throws FileNotFoundException, OdpsException {

    String filename = DownloadUtilsTest.class.getClassLoader().getResource("resource.py").getFile();
    PyResource rm = new PyResource();

    rm.setName("zhemin_res.py");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.py");
    assertEquals("type must be file", r.getType(), Type.PY);
  }

  private void addResourceArchive() throws FileNotFoundException, OdpsException {

    String filename = DownloadUtilsTest.class.getClassLoader().getResource("resource.tar.gz")
        .getFile();
    ArchiveResource rm = new ArchiveResource();

    rm.setName("zhemin_res.tar.gz");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.tar.gz");
    assertEquals("type must be file", r.getType(), Type.ARCHIVE);
  }

  private void addResourceTable() throws OdpsException {
    String tableName = "table_test";
    if (odps.tables().exists(tableName)) {
      odps.tables().delete(tableName);
    }
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("c1", OdpsType.BIGINT));
    odps.tables().create(tableName, schema);

    TableResource rm = new TableResource(tableName, odps.getDefaultProject());
    rm.setName("zhemin_res_src");

    odps.resources().create(rm);

    Resource r = odps.resources().get(rm.getName());
    Assert.assertEquals(Type.TABLE, r.getType());
    TableResource expect = (TableResource) r;
    Assert.assertEquals(odps.getDefaultProject(), expect.getProject());
    Assert.assertEquals(tableName, expect.getSourceTable().getName());
  }

  private void deleteResource(String resourceName) throws OdpsException {
    odps.resources().delete(resourceName);
  }

  @Before
  public void clean() {
    try {
      deleteResourceFile();
      deleteResource("zhemin_res.file");
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResource("zhemin_res.jar");
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResource("zhemin_res.py");
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResource("zhemin_res.tar.gz");
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResource("zhemin_res_src");
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResource("zhemin_res_pt");
    } catch (Exception e) {
      // pass
    }

    try {
      odps.tables().delete("src_pt");
      odps.tables().delete("src_pt");
    } catch (Exception e) {
      // pass
    }
  }

}
