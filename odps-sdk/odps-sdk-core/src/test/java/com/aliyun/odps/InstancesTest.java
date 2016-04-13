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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance.Result;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TunnelException;

public class InstancesTest extends TestBase {

  Instance i;
  private static String TABLE_NAME = InstancesTest.class.getSimpleName() + "_test_instances_test";

  @BeforeClass
  public static void setup() throws TunnelException, OdpsException, IOException {
    OdpsTestUtils.createTableForTest(TABLE_NAME);
  }

  @Before
  public void testCreate() throws OdpsException {
    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from " + TABLE_NAME + ";");
    task.setName("testsqlcase");
    i = odps.instances().create(task);
    i.getId();
    i.waitForSuccess();
    i.getTaskDetailJson("testsqlcase");
    // i.getOwner();
    // i.getStartTime();
    // i.getEndTime();
    // i.getProject();
    // i.getTaskNames();
    // i.isTerminated();
    // i.isSync();
    // i.stop();
  }

  @Test
  public void testExist() throws OdpsException {
    assertFalse(odps.instances().exists("not exist"));
    assertTrue(odps.instances().exists(i.getId()));
  }

  @Test
  public void testGetDetail() throws OdpsException {
    String details = i.getTaskDetailJson("testsqlcase");
    assertTrue("contains stage", details.contains("Stages"));
    assertTrue("contains stage", details.contains("Instance"));
  }

  @Test
  public void testGetTasks() throws OdpsException {
    List<Task> tasks = i.getTasks();
    assertTrue(tasks.size() == 1);
    assertTrue(tasks.get(0) instanceof SQLTask);
    SQLTask task = (SQLTask) tasks.get(0);
    assertTrue(task.getQuery().equals("select count(*) from " + TABLE_NAME + ";"));
  }

  @Test
  public void testList() throws OdpsException {
    int max = 50;
    for (Instance i : odps.instances()) {
      i.getId();
      i.getOwner();
      i.getStartTime();
      i.getProject();
      i.getTaskStatus();
      i.getTaskResults();
      --max;
      if (max < 0) {
        break;
      }
    }
  }

  @Test
  public void testListFilter() throws OdpsException {

    InstanceFilter filter = new InstanceFilter();
    filter.setOnlyOwner(false);
    // XXX need start different user instance
    int max = 50;
    Iterator<Instance> iter = odps.instances().iterator(filter);
    for (; iter.hasNext(); ) {
      Instance i = iter.next();
      System.out.println(i.getOwner());

      --max;
      if (max < 0) {
        break;
      }
    }

    max = 50;
    for (Instance instance : odps.instances().iterable(filter)) {
      assertNotNull(instance.getOwner());
      --max;
      if (max < 0) {
        break;
      }
    }
  }

  @Test
  public void testTaskResultsWithFormat() throws OdpsException {
    int max = 50;
    for (Instance i : odps.instances()) {
      Map<String, Result> results = i.getTaskResultsWithFormat();
      for (Result result : results.values()) {
        System.out.println(result.getFormat());
      }
      --max;
      if (max < 0) {
        break;
      }
    }
  }

  @Test
  public void testIterator() throws FileNotFoundException {
    Iterator<Instance> iterator = odps.instances().iterator();
    if (iterator.hasNext()) {
      iterator.next().getId();
    }
  }

  @Test
  public void testIterable() throws FileNotFoundException {
    for (Instance i : odps.instances().iterable()) {
      i.getId();
    }
  }

  @Test
  public void testCreateJob() throws OdpsException {
    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from " + TABLE_NAME + ";");
    task.setName("testsqlcase");
    Job job = new Job();
    job.addTask(task);
    i = odps.instances().create(job);
    i.getId();
    i.waitForSuccess();
    i.getTaskDetailJson("testsqlcase");
  }

}
