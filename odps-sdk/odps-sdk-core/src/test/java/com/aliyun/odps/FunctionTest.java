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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FunctionTest extends TestBase {

  public static final String FUNCTION_TEST = "function_test";
  public static final String CLASS_PATH = "function_test.class_path";
  public static final String RESOURCE_NAME = "function_resource.jar";
  private static final String UPDATE_RESOURCE_NAME = "update_function_resource.jar";
  private static final String FUNCTION_UPDATE_TEST = "function_update_test";
  private static final String UPDATE_CLASS_PATH = "update_function_test.update_class_path";

  @BeforeClass
  public static void createFunction() throws FileNotFoundException, OdpsException {
    clean();
    prepareResource();
    Function fm = new Function();
    fm.setName(FUNCTION_TEST);
    fm.setClassPath(CLASS_PATH);
    ArrayList<String> resources = new ArrayList<String>();
    resources.add(RESOURCE_NAME);
    fm.setResources(resources);
    odps.functions().create(fm);
  }

  private static void prepareResource() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader()
        .getResource("resource.jar").getFile();
    JarResource rm = new JarResource();
    rm.setName(RESOURCE_NAME);
    odps.resources().create(rm, new FileInputStream(new File(filename)));
    rm.setName(UPDATE_RESOURCE_NAME);
    odps.resources().create(rm, new FileInputStream(new File(filename)));
  }

  @AfterClass
  public static void clean() {
    deleteFunction();
    deleteResource();
  }

  @Test
  public void getFunction() throws OdpsException {
    Function function = odps.functions().get(FUNCTION_TEST);
    assertEquals(function.getClassPath(), CLASS_PATH);
    assertEquals(function.getResources().size(), 1);
    assertTrue(function.getResources().get(0).getName().endsWith(RESOURCE_NAME));
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
    System.out.println(ret.getResources().get(0).getName());
    assertTrue(ret.getResources().get(0).getName().endsWith(UPDATE_RESOURCE_NAME));

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

  public static void deleteFunction() {
    try {
      odps.functions().delete(FUNCTION_TEST);
    } catch (Exception e) {
    }

    try {
      odps.functions().delete(FUNCTION_UPDATE_TEST);
    } catch (Exception e) {
    }
  }

  private static void deleteResource() {
    try {
      odps.resources().delete(RESOURCE_NAME);
    } catch (Exception e) {
    }
    try {
      odps.resources().delete(UPDATE_RESOURCE_NAME);
    } catch (Exception e) {
    }
  }
}
