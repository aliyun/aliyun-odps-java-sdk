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

package com.aliyun.odps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Resource.Type;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.tunnel.VolumeTunnel;

public class ResourceTest extends TestBase {

  private static final String VOLUME_ARCHIVE_NAME = ResourceTest.class.getSimpleName() + "_volume_archive_for_test";
  private static final String tableName = ResourceTest.class.getSimpleName() + "_table_for_test_abc";
  private static final String TABLE_NAME = ResourceTest.class.getSimpleName() + "_test_resource_test";
  private static final String VOLUME_ARCHIVE_RESOURCE = "volume_archive_resource.jar";
  private static final String VOLUME_NAME = ResourceTest.class.getSimpleName() + "_volume_for_test";
  private static final String VOLUME_FILE_NAME = "volume_file_name.jar";
  private static final String VOLUME_UPDATE_FILE_NAME = "volume_update_file_name.jar";
  private static final String VOLUME_FILE_RESOURCE = "volume_resource";
  private static final String CHINESE_COMMENT_FILE = "chinese_comment_file";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    OdpsTestUtils.createTableForTest(TABLE_NAME);
    String grantUser = OdpsTestUtils.getGrantUser();
    if (!grantUser.toUpperCase().startsWith("ALIYUN$")) {
      grantUser = "ALIYUN$" + grantUser;
    }
    try {
      odps.projects().get().getSecurityManager().runQuery("grant admin to " + grantUser, false);
    } catch (OdpsException e) {
    }
  }

  @Test
  public void testUTF8Header() throws OdpsException, FileNotFoundException {

    if (odps.resources().exists(CHINESE_COMMENT_FILE)) {
      odps.resources().delete(CHINESE_COMMENT_FILE);
    }

    String filename = ResourceTest.class.getClassLoader().getResource("resource.txt").getFile();
    FileResource rm = new FileResource();

    rm.setName(CHINESE_COMMENT_FILE);
    rm.setComment("中文哈哈哈123");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get(CHINESE_COMMENT_FILE);
    assertEquals(r.getComment(), "中文哈哈哈123");

  }


  @Test
  public void testResourceFile() throws IOException, OdpsException {
    addResourceFile();
    updateResourceFile();
    listResources();
    deleteResourceFile();
    tempResource();
  }

  @Test
  public void testResourceJar() throws OdpsException, IOException {
    addResourceJar();
    updateResourceJar();
    listResources();
    deleteResourceJar();
  }

  @Test
  public void testResourcePy() throws OdpsException, IOException {
    addResourcePy();
    updateResourcePy();
    listResources();
    deleteResourcePy();
  }

  @Test
  public void testResourceArchive() throws OdpsException, IOException {
    addResourceArchive();
    updateResourceArchive();
    listResources();
    deleteResourceArchive();
  }

  @Test
  public void testResourceTable() throws FileNotFoundException, OdpsException {
    addResourceTable();
    updateResourceTable();
    listResources();
    deleteResourceTable();
  }

  @Test
  public void testResourceTablePartition() throws FileNotFoundException, OdpsException {
    addResourceTablePartition();
    updateResourceTablePartition();
    listResources();
    deleteResourceTablePartition();
  }

  @Test(expected = OdpsException.class)
  public void testResourceTableNameEmpty() throws OdpsException {
    TableResource resource = new TableResource(tableName, odps.getDefaultProject());
    odps.resources().create(resource);
  }

  @Test(expected = OdpsException.class)
  public void testResourceFileNameEmpty() throws OdpsException {
    FileResource resource = new FileResource();
    odps.resources().create(resource, null);
  }

  private void deleteResourceTablePartition() throws OdpsException {
    odps.resources().delete("zhemin_res_pt");
    odps.tables().delete(tableName);
  }

  private void addResourceTablePartition() throws OdpsException {
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

  private void updateResourceTablePartition() throws OdpsException {
    PartitionSpec pt = new PartitionSpec();
    pt.set("pt", "1");
    TableResource resource = new TableResource(tableName, odps.getDefaultProject(), pt);
    resource.setName("zhemin_res_pt");
    odps.resources().update(resource);

  }

  @Test
  public void testResourceMetaErrorMessage() throws OdpsException {
    String resName = "NotExists";
    try {
      odps.resources().get(resName).reload();
    } catch (OdpsException e) {
      assertTrue(e.getMessage().contains("ODPS-"));
    }
  }


  private void addResourceFile() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader().getResource("resource.txt").getFile();
    FileResource rm = new FileResource();

    rm.setName("zhemin_res.file");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.file");
    assertEquals("type must be file", r.getType(), Type.FILE);
  }


  private void updateResourceFile() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader().getResource("resource.txt").getFile();
    FileResource rm = new FileResource();
    rm.setName("zhemin_res.file");
    odps.resources().update(rm, new FileInputStream(new File(filename)));
    Resource r = odps.resources().get("zhemin_res.file");
    assertEquals("type must be file", r.getType(), Type.FILE);
    assertEquals("zhemin_res.file", r.getName());
    assertEquals(Long.valueOf(new File(filename).length()), Long.valueOf(r.getSize()));
    assertNotNull(r.getLastUpdator());
  }

  private void deleteResourceFile() throws OdpsException {
    odps.resources().delete("zhemin_res.file");
  }

  private void addResourceJar() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader().getResource("resource.jar").getFile();
    JarResource rm = new JarResource();

    rm.setName("zhemin_res.jar");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.jar");
    assertEquals("type must be file", r.getType(), Type.JAR);
  }

  private void addResourcePy() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader().getResource("resource.py").getFile();
    PyResource rm = new PyResource();

    rm.setName("zhemin_res.py");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.py");
    assertEquals("type must be file", r.getType(), Type.PY);
  }

  private void addResourceArchive() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader().getResource("resource.tar.gz").getFile();
    ArchiveResource rm = new ArchiveResource();

    rm.setName("zhemin_res.tar.gz");
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.tar.gz");
    assertEquals("type must be file", r.getType(), Type.ARCHIVE);
  }

  private void updateResourceJar() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader().getResource("resource.jar").getFile();
    JarResource rm = new JarResource();
    rm.setName("zhemin_res.jar");
    odps.resources().update(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.jar");
    assertEquals(r.getType(), Type.JAR);
    assertEquals("zhemin_res.jar", r.getName());
    assertNotNull(r.getSize());
    assertNotNull(r.getLastUpdator());
  }

  private void updateResourcePy() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader().getResource("resource.py").getFile();
    PyResource rm = new PyResource();
    rm.setName("zhemin_res.py");
    odps.resources().update(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.py");
    assertEquals(r.getType(), Type.PY);
    assertEquals("zhemin_res.py", r.getName());
    assertNotNull(r.getSize());
    assertNotNull(r.getLastUpdator());
  }

  private void updateResourceArchive() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader().getResource("resource.tar.gz").getFile();
    ArchiveResource rm = new ArchiveResource();
    rm.setName("zhemin_res.tar.gz");
    odps.resources().update(rm, new FileInputStream(new File(filename)));

    Resource r = odps.resources().get("zhemin_res.tar.gz");
    assertEquals(r.getType(), Type.ARCHIVE);
    assertEquals("zhemin_res.tar.gz", r.getName());
    assertNotNull(r.getSize());
    assertNotNull(r.getLastUpdator());
  }

  private void deleteResourceJar() throws OdpsException {
    odps.resources().delete("zhemin_res.jar");
  }

  private void deleteResourcePy() throws OdpsException {
    odps.resources().delete("zhemin_res.py");
  }

  private void deleteResourceArchive() throws OdpsException {
    odps.resources().delete("zhemin_res.tar.gz");
  }

  private void addResourceTable() throws OdpsException {
    TableResource rm = new TableResource(TABLE_NAME, odps.getDefaultProject());
    rm.setName("zhemin_res_src");

    odps.resources().create(rm);

    Resource r = odps.resources().get(rm.getName());
    Assert.assertEquals(Type.TABLE, r.getType());
    TableResource expect = (TableResource) r;
    Assert.assertEquals(odps.getDefaultProject(), expect.getProject());
    Assert.assertEquals(TABLE_NAME, expect.getSourceTable().getName());
  }

  private void updateResourceTable() throws OdpsException {
    TableResource rm = new TableResource(TABLE_NAME, odps.getDefaultProject());
    rm.setName("zhemin_res_src");
    odps.resources().update(rm);

    Resource r = odps.resources().get("zhemin_res_src");
    assertEquals(r.getType(), Type.TABLE);
    assertEquals("zhemin_res_src", r.getName());
    assertNull(r.getSize());
    assertNotNull(r.getLastUpdator());
  }

  private void deleteResourceTable() throws OdpsException {
    odps.resources().delete("zhemin_res_src");
  }

  private void listResources() {
    int size = 0;
    for (Resource a : odps.resources()) {
      ++size;
    }

    assertTrue("size > 0", size > 0);
  }

  private void tempResource() throws OdpsException, IOException {
    String filename = ResourceTest.class.getClassLoader().getResource("resource.jar").getFile();
    FileResource rm = new FileResource();

    try {
      odps.resources().delete("zhemin_temp.jar");
    } catch (Exception e) {
      // pass
    }

    rm.setName("zhemin_temp.jar");

    rm.setIsTempResource(true);
    odps.resources().create(rm, new FileInputStream(new File(filename)));

    for (Resource a : odps.resources()) {
      if ("zhemin_temp.jar".equals(a.getName())) {
        fail("list resource find temp resource");
      }
    }
    odps.resources().delete("zhemin_temp.jar");
  }

  @Test
  public void downloadResource() throws OdpsException, IOException {
    String filename = ResourceTest.class.getClassLoader().getResource("resource.jar").getFile();
    JarResource rm = new JarResource();

    rm.setName("zhemin_res.jar");
    odps.resources().create(rm, new FileInputStream(new File(filename)));
    InputStream in = odps.resources().getResourceAsStream("zhemin_res.jar");
    InputStream origis = new FileInputStream(new File(filename));
    byte[] orig = new byte[8196];
    byte[] down = new byte[8196];
    while (in.available() > 0) {
      in.read(down);
      origis.read(orig);
      Assert.assertArrayEquals(orig, down);
    }
    assertTrue(origis.available() == 0);
    origis.close();
    odps.resources().delete("zhemin_res.jar");
  }

  @Test(expected = OdpsException.class)
  public void downloadResourceNegative() throws OdpsException {
    InputStream in = odps.resources().getResourceAsStream("not_exist.jar");
  }

  @Before
  public void clean() {
    try {
      deleteResourceFile();
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResourceJar();
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResourcePy();
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResourceArchive();
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResourceTable();
    } catch (Exception e) {
      // pass
    }
    try {
      deleteResourceTablePartition();
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
