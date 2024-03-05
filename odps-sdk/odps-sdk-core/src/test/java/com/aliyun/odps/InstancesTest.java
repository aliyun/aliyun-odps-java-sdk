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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.aliyun.odps.Instance.InstanceResultModel;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.Instance.Result;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TunnelException;

public class InstancesTest extends TestBase {

  Instance gi;
  private static String TABLE_NAME = InstancesTest.class.getSimpleName() + "_test_instances_test";
  private static String TABLE_NAME_1 = InstancesTest.class.getSimpleName() + "_instances_test_1";

  @BeforeClass
  public static void setup() throws TunnelException, OdpsException, IOException {
    OdpsTestUtils.createTableForTest(TABLE_NAME);
    OdpsTestUtils.createBigTableForTest(odps, TABLE_NAME_1);
  }

  @Before
  public void testCreate() throws OdpsException {
    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from " + TABLE_NAME + ";");
    task.setName("testsqlcase");
    gi = odps.instances().create(task);
    gi.getId();
    gi.waitForSuccess();
    gi.getTaskDetailJson("testsqlcase");
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
    assertTrue(odps.instances().exists(gi.getId()));
  }

  @Test
  public void testGetDetail() throws OdpsException {
    String details = gi.getTaskDetailJson("testsqlcase");
    assertTrue("contains stage", details.contains("Stages"));
    assertTrue("contains stage", details.contains("Instance"));
  }

  @Ignore // 公共云后付费项目才有此字段
  @Test
  public void testGetTaskCost() throws OdpsException {
    try {
      Instance.TaskCost cost = gi.getTaskCost("testsqlcase");
      Assert.assertTrue(cost.getCPUCost() >= 0);
    } catch (Exception ignore){}
  }


  @Test
  public void testGetTasks() throws OdpsException {
    List<Task> tasks = gi.getTasks();
    assertTrue(tasks.size() == 1);
    assertTrue(tasks.get(0) instanceof SQLTask);
    SQLTask task = (SQLTask) tasks.get(0);
    assertTrue(task.getQuery().equals("select count(*) from " + TABLE_NAME + ";"));
  }

  @Test
  public void testList() throws OdpsException {
    int max = 5;
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
    int max = 5;
    Iterator<Instance> iter = odps.instances().iterator(filter);
    for (; iter.hasNext(); ) {
      Instance i = iter.next();
      System.out.println(i.getOwner());

      --max;
      if (max < 0) {
        break;
      }
    }

    max = 5;
    for (Instance instance : odps.instances().iterable(filter)) {
      assertNotNull(instance.getOwner());
      --max;
      if (max < 0) {
        break;
      }
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDateIllegal() throws OdpsException {
    InstanceFilter filter = new InstanceFilter();
    filter.setOnlyOwner(true);
    filter.setFromTime(new Date());
    filter.setEndTime(new Date());

    odps.instances().iterator(filter).hasNext();
  }

  @Test
  public void testTaskResultsWithFormat() throws OdpsException {
    int max = 5;
    for (Instance i : odps.instances()) {
      Map<String, Result> results = new HashMap<String, Result>();
      try {
         results = i.getTaskResultsWithFormat();
      } catch (Exception e) {
        e.printStackTrace();
      }
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
  public void testRawTaskResults() throws OdpsException {
    Instance i = SQLTask.run(odps, "select count(*) from " + TABLE_NAME + ";");
    i.waitForSuccess();
    InstanceResultModel.TaskResult taskResult = i.getRawTaskResults().get(0);
    Instance.TaskStatus.Status taskStatus =
        Instance.TaskStatus.Status.valueOf(taskResult.status.toUpperCase());
    assertEquals(Instance.TaskStatus.Status.SUCCESS, taskStatus);
    assertNotNull(taskResult.name);
    assertNotNull(taskResult.type);
    assertNotNull(taskResult.result.text);
  }

  @Test
  public void testFailedRawTaskResults() throws OdpsException {
    Instance i = SQLTask.run(odps, "select count(*) from table_not_exists;");
    try {
      i.waitForSuccess();
    } catch (OdpsException e) {
      // ignore
    }
    InstanceResultModel.TaskResult taskResult = i.getRawTaskResults().get(0);
    Instance.TaskStatus.Status taskStatus =
        Instance.TaskStatus.Status.valueOf(taskResult.status.toUpperCase());

    assertEquals(Instance.TaskStatus.Status.FAILED, taskStatus);
    assertNotNull(taskResult.name);
    assertNotNull(taskResult.type);
    assertNotNull(taskResult.result.text);
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
    int maxIter = 5;
    int cnt = 0;
    for (Instance i :odps.instances().iterable()) {
      i.getId();
      if (cnt++ > maxIter) {
        break;
      }
    }
  }

  @Test
  public void testCreateJob() throws OdpsException {
    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from " + TABLE_NAME + ";");
    task.setName("testsqlcase");
    Job job = new Job();
    job.addTask(task);
    gi = odps.instances().create(job);
    gi.getId();
    gi.waitForSuccess();
    gi.getTaskDetailJson("testsqlcase");
    Assert.assertNull(gi.getJobName());

    String jobName = "test_job_name";
    job.setName(jobName);
    gi = odps.instances().create(job);
    gi.waitForSuccess();
    Assert.assertEquals(jobName, gi.getJobName());
  }

  @Test
  public void testCreateJobName() throws OdpsException {
    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from " + TABLE_NAME + ";");
    task.setName("testsqlcase");

    String jobName = "test_job_name";
    gi = odps.instances().create(odps.getDefaultProject(), task, null, null, jobName);
    gi.getId();
    gi.waitForSuccess();
    gi.getTaskDetailJson("testsqlcase");
    Assert.assertEquals(jobName, gi.getJobName());
  }


  @Test
  public void testCreateSyncInstance() throws OdpsException {
    // suppose create table is a sync instance
    String name = OdpsTestUtils.getRandomName();
    String taskname = "testSyncInstance";

    // success instance
    SQLTask task = new SQLTask();
    task.setQuery("create table if not exists " + name + " (test string);");
    task.setName(taskname);
    Instance i = odps.instances().create(task);

    assertTrue(i.isSuccessful());
    assertTrue(i.isSync());
    assertFalse(i.getTaskResults().isEmpty());
    assertTrue(i.isSuccessful()); // test hasTaskStatus

    String result = i.getTaskResults().get(taskname);
    assertNotNull(result);
    System.out.println(result);

    // failed instance
    task = new SQLTask();
    task.setQuery("create table " + name + " (test string);");
    task.setName(taskname);
    i = odps.instances().create(task);

    assertFalse(i.isSuccessful());
    assertTrue(i.isSync());
    assertFalse(i.getTaskResults().isEmpty());
    assertFalse(i.isSuccessful()); // test hasTaskStatus

    result = i.getTaskResults().get(taskname);
    assertNotNull(result);
    System.out.println(result);

    odps.tables().delete(name, true);
  }

  @Test
  public void testIteratorQueueing() throws Exception {
    SQLTask task = new SQLTask();
    task.setQuery("select (t2.c1 + 2) from " + TABLE_NAME + " t1 join " + TABLE_NAME_1 + " t2 on t1.c1 == t2.c1;");
    task.setName("testsqlcase");

    // Since the instance may have passed the queueing stage when calling iteratorQueueing#, here
    // we submit 10 instances to make sure the last instances submitted is queueing, not 100% though
    for (int counter = 0; counter < 10; counter++) {
      gi = odps.instances().create(task);
    }

    System.out.println("Now create Instance: " + gi.getId());

    Iterator<Instance.InstanceQueueingInfo> iterator = odps.instances().iteratorQueueing();

    Assert.assertTrue(iterator.hasNext());

    while (iterator.hasNext()) {
      Instance.InstanceQueueingInfo info = iterator.next();
      System.out.println(info.getId());
      System.out.println(info.getStatus());
      System.out.println(info.getStartTime());
      System.out.println(info.getProgress());

      if (gi.getId().equals(info.getId())) {
        Assert.assertNotNull(info.getPriority());
        Assert.assertNotNull(info.getProgress());
        Assert.assertNotNull(info.getTaskName());
        Assert.assertNotNull(info.getTaskType());
        Assert.assertNotNull(info.getStartTime());
        Assert.assertNotNull(info.getStatus());
        Assert.assertNotNull(info.getProject());
        Assert.assertNotNull(info.getUserAccount());
        Assert.assertNotNull(info.getId());
      }
    }

  }

  @Test
  public void testGetQueueingInfo() throws Exception {
    SQLTask task = new SQLTask();
    task.setQuery("select (t2.c1 + 2) from " + TABLE_NAME + " t1 join " + TABLE_NAME_1
                  + " t2 on t1.c1 == t2.c1;");
    task.setName("testsqlcase");
    gi = odps.instances().create(task);
    // because of environment diff， we don't know how much time it takes
    Instance.InstanceQueueingInfo info = null;
    while (gi.getStatus() != Instance.Status.TERMINATED) {
      Thread.sleep(500);
      info = gi.getQueueingInfo();
      Map substatus = info.getProperty("subStatus", Map.class);
      if (substatus != null) {
        break;
      }
    }
    Map substatus = info.getProperty("subStatus", Map.class);
    System.out.println(substatus.get("start_time"));
    System.out.println(substatus.get("description"));

    System.out.println(info.getId());
    System.out.println(info.getStatus());
    System.out.println(info.getStartTime());
    System.out.println(info.getProgress());

    Assert.assertEquals(gi.getId(), info.getId());
    Assert.assertNotNull(info.getPriority());
    Assert.assertNotNull(info.getTaskName());
    Assert.assertNotNull(info.getTaskType());
    Assert.assertNotNull(info.getStartTime());
    Assert.assertNotNull(info.getStatus());
    Assert.assertNotNull(info.getProject());
    Assert.assertNotNull(info.getUserAccount());

    gi.waitForSuccess();
    for (int j = 0; j < 15; j++) {
      info = gi.getQueueingInfo();
      if (info.getId() != null) {
        break;
      }
      Thread.sleep(1000);
    }
    if (info.getId() == null) {
      return;
    }
    Assert.assertEquals(gi.getId(), info.getId());
  }
}
