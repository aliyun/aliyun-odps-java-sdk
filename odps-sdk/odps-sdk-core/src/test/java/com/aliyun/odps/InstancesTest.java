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
import com.aliyun.odps.rest.SimpleXmlUtils;
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
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

public class InstancesTest extends TestBase {

  Instance i;
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
    int max = 50;
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
    Assert.assertNull(i.getJobName());

    String jobName = "test_job_name";
    job.setName(jobName);
    i = odps.instances().create(job);
    i.waitForSuccess();
    Assert.assertEquals(jobName, i.getJobName());
  }

  @Test
  public void testCreateJobName() throws OdpsException {
    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from " + TABLE_NAME + ";");
    task.setName("testsqlcase");

    String jobName = "test_job_name";
    i = odps.instances().create(odps.getDefaultProject(), task, null, null, jobName);
    i.getId();
    i.waitForSuccess();
    i.getTaskDetailJson("testsqlcase");
    Assert.assertEquals(jobName, i.getJobName());
  }


  @Test
  public void testCreateSyncInstance() throws OdpsException {
    // suppose create table is a sync instance
    String name = OdpsTestUtils.getRandomTableName();
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
      i = odps.instances().create(task);
    }

    System.out.println("Now create Instance: " + i.getId());

    Iterator<Instance.InstanceQueueingInfo> iterator = odps.instances().iteratorQueueing();

    Assert.assertTrue(iterator.hasNext());

    while (iterator.hasNext()) {
      Instance.InstanceQueueingInfo info = iterator.next();
      System.out.println(info.getId());
      System.out.println(info.getStatus());
      System.out.println(info.getStartTime());
      System.out.println(info.getProgress());

      if (i.getId().equals(info.getId())) {
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
    i = odps.instances().create(task);

    Thread.sleep(1000);
    Instance.InstanceQueueingInfo info = i.getQueueingInfo();
    Map substatus = info.getProperty("subStatus", Map.class);
    System.out.println(substatus.get("start_time"));
    System.out.println(substatus.get("description"));

    System.out.println(info.getId());
    System.out.println(info.getStatus());
    System.out.println(info.getStartTime());
    System.out.println(info.getProgress());

    Assert.assertEquals(i.getId(), info.getId());
    Assert.assertNotNull(info.getPriority());
    Assert.assertNotNull(info.getProgress());
    Assert.assertNotNull(info.getTaskName());
    Assert.assertNotNull(info.getTaskType());
    Assert.assertNotNull(info.getStartTime());
    Assert.assertNotNull(info.getStatus());
    Assert.assertNotNull(info.getProject());
    Assert.assertNotNull(info.getUserAccount());

    i.waitForSuccess();
    Thread.sleep(3000);
    info = i.getQueueingInfo();
    Assert.assertEquals(i.getId(), info.getId());
  }
}
