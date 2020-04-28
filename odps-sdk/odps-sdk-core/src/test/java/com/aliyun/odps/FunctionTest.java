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

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Objects;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;

public class FunctionTest extends TestBase {

  public static final String FUNCTION_TEST = "function_test";
  public static final String SQL_FUNCTION_TEST = "sql_function_test";
  public static final String CLASS_PATH = "function_test.class_path";
  public static final String RESOURCE_NAME = "function_resource.jar";
  private static final String UPDATE_RESOURCE_NAME = "update_function_resource.jar";
  private static final String FUNCTION_UPDATE_TEST = "function_update_test";
  private static final String UPDATE_CLASS_PATH = "update_function_test.update_class_path";
  private static String grantUser;

  private static final String SQL_FUNCTION_DEFINITION_TEXT =
      String.format("CREATE SQL FUNCTION %s(@a bigint, @b bigint) as @a + @b;", SQL_FUNCTION_TEST);

  @BeforeClass
  public static void beforeClass() throws FileNotFoundException, OdpsException {
    deleteFunction();
    deleteResource();
    createFunction();
  }

  public static void createFunction() throws FileNotFoundException, OdpsException {
    prepareResource();
    Function fm = new Function();
    fm.setName(FUNCTION_TEST);
    fm.setClassPath(CLASS_PATH);
    ArrayList<String> resources = new ArrayList<String>();
    resources.add(RESOURCE_NAME);
    fm.setResources(resources);
    odps.functions().create(fm);
    grantUser = OdpsTestUtils.getGrantUser();
    if (!grantUser.toUpperCase().startsWith("ALIYUN$")) {
      grantUser = "ALIYUN$" + grantUser;
    }
    try {
      odps.projects().get().getSecurityManager().runQuery("grant admin to " + grantUser, false);
    } catch (OdpsException e) {
    }

    // Create a SQL function
    Instance createSqlFunc  = SQLTask.run(odps, SQL_FUNCTION_DEFINITION_TEXT);
    createSqlFunc.waitForSuccess();
  }

  private static void prepareResource() throws FileNotFoundException, OdpsException {

    String filename = Objects.
        requireNonNull(ResourceTest.class.getClassLoader().getResource("resource.jar"))
        .getFile();
    JarResource rm = new JarResource();
    rm.setName(RESOURCE_NAME);
    odps.resources().create(rm, new FileInputStream(new File(filename)));
    rm.setName(UPDATE_RESOURCE_NAME);
    odps.resources().create(rm, new FileInputStream(new File(filename)));
  }

  @AfterClass
  public static void afterClass() {
    deleteFunction();
    deleteResource();
  }

  public static void deleteFunction() {
    try {
      odps.functions().delete(FUNCTION_TEST);
    } catch (Exception e) {
      // Ignore
    }

    try {
      odps.functions().delete(FUNCTION_UPDATE_TEST);
    } catch (Exception e) {
      // Ignore
    }

    try {
      odps.functions().delete(SQL_FUNCTION_TEST);
    } catch (Exception e) {
      // Ignore
    }
  }

  private static void deleteResource() {
    try {
      odps.resources().delete(RESOURCE_NAME);

    } catch (Exception e) {
      // Ignore
    }

    try {
      odps.resources().delete(UPDATE_RESOURCE_NAME);

    } catch (Exception e) {
      // Ignore
    }
  }

  @Test
  public void getFunction() throws OdpsException {
    Function function = odps.functions().get(FUNCTION_TEST);
    assertEquals(CLASS_PATH, function.getClassPath());
    assertEquals(1, function.getResources().size());
    assertEquals(Resource.Type.JAR, function.getResources().get(0).getType());
    assertEquals(RESOURCE_NAME, function.getResources().get(0).getName());
    assertEquals(1, function.getResourceNames().size());
    assertEquals(RESOURCE_NAME, function.getResourceNames().get(0));
    assertFalse(function.isSqlFunction());
    assertNull(function.getSqlDefinitionText());
  }

  @Test
  public void getSqlFunction() throws OdpsException {
    Function function = odps.functions().get(SQL_FUNCTION_TEST);
    assertEquals("", function.getClassPath());
    assertEquals(0, function.getResources().size());
    assertEquals(0, function.getResourceNames().size());
    assertTrue(function.isSqlFunction());
    assertEquals(SQL_FUNCTION_DEFINITION_TEXT, function.getSqlDefinitionText() + ";");
  }


  @Test(expected = NoSuchObjectException.class)
  public void getFunctionNotExist() throws OdpsException {
    Function function = odps.functions().get("NOT_EXISTS");
    function.reload();
  }


  @Test
  public void testExists() throws OdpsException {
    assertTrue(odps.functions().exists(FUNCTION_TEST));
    assertFalse(odps.functions().exists("NOT_EXISTS"));
  }

  @Test
  public void updateFunction() throws OdpsException {
    Function fm = new Function();
    fm.setName(FUNCTION_UPDATE_TEST);
    fm.setClassPath(CLASS_PATH);
    ArrayList<String> resources = new ArrayList<String>();
    resources.add(RESOURCE_NAME);
    fm.setResources(resources);
    odps.functions().create(fm);

    fm.setClassPath(UPDATE_CLASS_PATH);
    resources.set(0, UPDATE_RESOURCE_NAME);
    fm.setResources(resources);

    odps.functions().update(fm);

    Function ret = odps.functions().get(FUNCTION_UPDATE_TEST);
    assertEquals(ret.getClassPath(), UPDATE_CLASS_PATH);
    assertEquals(ret.getResources().size(), 1);
    assertEquals(ret.getResourceNames().size(), 1);
    System.out.println(ret.getResources().get(0).getName());
    assertTrue(ret.getResources().get(0).getName().endsWith(UPDATE_RESOURCE_NAME));
    assertTrue(ret.getResourceNames().get(0).endsWith(UPDATE_RESOURCE_NAME));
  }

  @Test
  public void listFunction() {
    int count = 0;
    for (Function f : odps.functions()) {
      ++count;
      Assert.assertNotNull(f.getOwner());
    }
    assertTrue("function nums > 0 ", count > 0);
  }
}
