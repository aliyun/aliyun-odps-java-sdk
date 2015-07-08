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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class FunctionTest extends TestBase {


  @Test
  public void test() throws Exception {
    // create function
    createFunction();
    // list function
    listFunction();
    // delete function
    deleteFunction();
  }

  private void createFunction() throws FileNotFoundException, OdpsException {
    prepareResource();
    Function fm = new Function();
    fm.setName("zhemin_fun");
    fm.setClassPath("zhemin.fun");
    ArrayList<String> resources = new ArrayList<String>();
    resources.add("zhemin_fun.jar");
    fm.setResources(resources);
    odps.functions().create(fm);
  }

  private void prepareResource() throws FileNotFoundException, OdpsException {

    String filename = ResourceTest.class.getClassLoader()
        .getResource("resource.jar").getFile();
    JarResource rm = new JarResource();
    rm.setName("zhemin_fun.jar");
    odps.resources().create(rm, new FileInputStream(new File(filename)));
  }

  @After
  public void clean() {
    try {
      deleteFunction();
    } catch (Exception e) {
      // pass
    }

    try {
      deleteResource();
    } catch (Exception e) {
      // pass
    }
  }

  private void listFunction() {
    int count = 0;
    for (Function f : odps.functions()) {
      ++count;
      Assert.assertNotNull(f.getOwner());
    }
    assertTrue("function nums > 0 ", count > 0);
  }

  private void deleteFunction() throws Exception {
    odps.functions().delete("zhemin_fun");
  }

  private void deleteResource() throws OdpsException {
    odps.resources().delete("zhemin_fun.jar");
  }
}
