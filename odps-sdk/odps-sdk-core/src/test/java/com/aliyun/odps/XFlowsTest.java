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
import static org.junit.Assert.assertTrue;

import java.util.Map;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.XFlow.XFlowModel;
import com.aliyun.odps.XFlows.XFlowInstance;
import com.aliyun.odps.XFlows.XResult;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.task.SQLTask;

public class XFlowsTest {

  private static Odps odps;

  @BeforeClass
  public static void setup() throws OdpsException {
    odps = OdpsTestUtils.newTestOdps();
    testCreateDeleteUpadte();
    if (!odps.xFlows().exists("abc")) {
      String source =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xflow xmlns=\"odps:xflow:0.1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" name=\"abc\" comments=\"String\" catalog=\"String\" xsi:schemaLocation=\"odps:xflow:0.1xflow.xsd\"><workflow><start to=\"A\"/><end name=\"A\"/></workflow></xflow>";
      XFlowModel model = new XFlowModel();
      model.setXmlSource(source);
      model.setName("abc");
      odps.xFlows().create(model);

    }

  }

  @Test
  public void testGetXFlows() {
    XFlows xflows = odps.xFlows();
    XFlow xflow = xflows.get("abc");
    assertEquals(xflow.getName(), "abc");
    assertTrue(xflow.getSourceXml().contains("workflow"));
  }

  @Test
  public void testexistsXFlow() throws OdpsException {
    XFlows xflows = odps.xFlows();
    assertTrue(xflows.exists("abc"));
    assertFalse(xflows.exists("NOT_EXSIST"));
  }

  private static final String TEST_XFLOW = "test_xflow";


  public static void testCreateDeleteUpadte() throws OdpsException {
    XFlows xflows = odps.xFlows();
    if (xflows.exists(TEST_XFLOW)) {
      xflows.delete(TEST_XFLOW);
    }
    String
        source_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xflow xmlns=\"odps:xflow:0.1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" name=\"test_xflow\" comments=\"String\" catalog=\"String\" xsi:schemaLocation=\"odps:xflow:0.1xflow.xsd\"><workflow><start to=\"A\"/><end name=\"A\"/></workflow></xflow>";
    XFlowModel model = new XFlowModel();
    model.setXmlSource(source_xml);
    model.setName(TEST_XFLOW);
    xflows.create(model);
    assertEquals(xflows.get(TEST_XFLOW).getSourceXml(), source_xml);

    String
        update_xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><xflow xmlns=\"odps:xflow:0.1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" name=\"test_xflow\" comments=\"String\" catalog=\"String\" xsi:schemaLocation=\"odps:xflow:0.1xflow.xsd\"><workflow><start to=\"ABC\"/><end name=\"ABC\"/></workflow></xflow>";
    model.setXmlSource(update_xml);
    model.setName(TEST_XFLOW);
    xflows.update(model);
    assertEquals(xflows.get(TEST_XFLOW).getSourceXml(), update_xml);
  }

  @Test
  public void testIterator() throws OdpsException {
    XFlows xflows = odps.xFlows();
    long cnt = 0;
    for (XFlow xflow : xflows) {
      System.out.println(xflow.getName());
      System.out.println(xflow.getSourceXml());
      ++cnt;
      if (cnt > 2) {
        break;
      }
    }
  }

  @Test
  public void testExecuteXFlow() throws OdpsException, InterruptedException {
    XFlows xflows = odps.xFlows();
    XFlowInstance xFlowInstance = new XFlowInstance();
    xFlowInstance.setXflowName(TEST_XFLOW);
    xFlowInstance.setProject(odps.getDefaultProject());
    xFlowInstance.setPriority(8);
    xFlowInstance.setProperty("key1", "value1");
    Instance i = xflows.execute(xFlowInstance);
    System.out.println(i.getId());
    i.getStatus();
    i.getTaskStatus();
    i.waitForSuccess();

    Assert.assertTrue(xflows.isXFlowInstance(i));
    XFlowInstance xFlowInstance2 = xflows.getXFlowInstance(i);
    Assert.assertEquals(xFlowInstance.getXflowName(), xFlowInstance2.getXflowName());
    Assert.assertEquals(xFlowInstance.getProject(), xFlowInstance2.getProject());
    Assert.assertEquals(xFlowInstance.getPriority(), xFlowInstance2.getPriority());

    System.out.println("XSOURCE" + xflows.getXSource(i));
    System.out.println(i.getStatus());
    System.out.println(i.getTasks());

    testGetXResult(i);
  }

  @Test
  public void testIsNotXFlowInstance() throws OdpsException {
    Instance instance = SQLTask.run(odps, "select count(*) from src;");
    Assert.assertFalse(odps.xFlows().isXFlowInstance(instance));
  }

  public void testGetXResult(Instance i) throws OdpsException {
    XFlows xflows = odps.xFlows();
    Map<String, XResult> xResults = xflows.getXResults(i);
    for (XResult xResult : xResults.values()) {
      assertTrue(xResult.getInstanceId() != null);
      System.out.println(xResult.getInstanceId());
      System.out.println(xResult.getName());
      System.out.println(xResult.getResult());
      System.out.println(xResult.getNodeType());
    }
  }

  @Test
  public void testXFlowCData() throws JAXBException {
    XFlowInstance instance = new XFlowInstance();
    instance.setParameter("abc", "\"<>\"");
    instance.setPriority(2);
    instance.setProperty("key1", "value1");
    instance.setProperty("key2", "value2");
    String st = JAXBUtils.marshal(instance, XFlowInstance.class);
    assertEquals(st, "<?xml version=\"1.0\" ?>" +
            "<XflowInstance><Parameters><Parameter><Key>abc</Key><Value><![CDATA[\"<>\"]]>" +
            "</Value></Parameter></Parameters><Priority>2</Priority>" +
            "<Config><Property><Name>key1</Name><Value>value1</Value></Property>" +
            "<Property><Name>key2</Name><Value>value2</Value></Property>" +
            "</Config></XflowInstance>");
  }
}
