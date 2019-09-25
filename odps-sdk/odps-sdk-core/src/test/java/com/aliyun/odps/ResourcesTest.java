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
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

public class ResourcesTest extends TestBase {

  @Test
  public void testIterator() {
    Iterator<Resource> iterator = odps.resources().iterator();
    if (iterator.hasNext()) {
      iterator.next().getName();
    }
  }

  @Test
  public void testIterable() {
    for (Resource resource : odps.resources().iterable()) {
      assertNotNull(resource.getName());
    }
  }

  public static <E> Collection<E> makeCollection(Iterable<E> iter) {
    Collection<E> list = new ArrayList<E>();
    for (E item : iter) {
      list.add(item);
    }
    return list;
  }

  @Test
  public void testTempResource() throws OdpsException {
    URL testFile = Odps.class.getClassLoader().getResource("resource.txt");
    assertNotNull(testFile);
    FileResource r = odps.resources().createTempResource(testFile.getFile());
    assertTrue(r instanceof FileResource);
    assertTrue(r.getName().endsWith("resource.txt"));
    assertEquals(r.getType(), Resource.Type.FILE);
  }

  @Test(expected = OdpsException.class)
  public void testNotExists() throws OdpsException {
    FileResource r = odps.resources().createTempResource("abcdef");
  }

  @Test
  public void testTempJar() throws OdpsException {
    URL testFile = Odps.class.getClassLoader().getResource("resource.jar");
    assertNotNull(testFile);
    FileResource
        r =
         odps.resources().createTempResource(odps.getDefaultProject(), testFile.getFile(),
                                            Resource.Type.JAR);
    assertTrue(r instanceof JarResource);
    assertTrue(r.getName().endsWith("resource.jar"));
    assertEquals(r.getType(), Resource.Type.JAR);
  }

  @Test
  public void testTempPY() throws OdpsException {
    URL testFile = Odps.class.getClassLoader().getResource("resource.py");
    assertNotNull(testFile);
    FileResource
        r =
        odps.resources().createTempResource(odps.getDefaultProject(), testFile.getFile(),
                                            Resource.Type.PY);
    assertTrue(r instanceof PyResource);
    assertTrue(r.getName().endsWith("resource.py"));
    assertEquals(r.getType(), Resource.Type.PY);
  }

  @Test(expected = NullPointerException.class)
  public void testTypeNull() throws OdpsException {
    URL testFile = Odps.class.getClassLoader().getResource("resource.jar");
    assertNotNull(testFile);
    FileResource
        r =
        odps.resources().createTempResource(odps.getDefaultProject(), testFile.getFile(), null);
  }

  @Test(expected = NullPointerException.class)
  public void testFileNull() throws OdpsException {
    FileResource r = odps.resources().createTempResource(null);
  }
}
