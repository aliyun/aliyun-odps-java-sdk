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

import com.aliyun.odps.Project.Cluster;
import com.aliyun.odps.Project.Clusters;
import com.aliyun.odps.Project.ProjectModel;
import com.aliyun.odps.rest.SimpleXmlUtils;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

public class ProjectTest extends TestBase {
  private String xml =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<Project>\n"
      + "   <Name>odps_mingyou_test</Name>\n"
      + "   <Type>managed</Type>\n"
      + "   <Comment>project copy from [odps_test_tunnel_project]</Comment>\n"
      + "   <ProjectGroupName>group_200</ProjectGroupName>\n"
      + "   <Properties>\n"
      + "      <Property>\n"
      + "         <Name>odps.security.ip.whitelist</Name>\n"
      + "         <Value></Value>\n"
      + "      </Property>\n"
      + "      <Property>\n"
      + "         <Name>property2</Name>\n"
      + "         <Value>val</Value>\n"
      + "      </Property>\n"
      + "   </Properties>\n"
      + "   <State>AVAILABLE</State>\n"
      + "   <Clusters>\n"
      + "      <Cluster>\n"
      + "         <Name>AT-200N</Name>\n"
      + "         <QuotaID>1</QuotaID>\n"
      + "      </Cluster>\n"
      + "   </Clusters>\n"
      + "</Project>";

  @Test
  public void testGetName() throws OdpsException {
    boolean flag = odps.projects().exists("project_not_exist");
    assertEquals(false, flag);
  }

  @Test
  public void testGetExtendedProperties() throws Exception {
    Map<String, String> properties = odps.projects().get().getExtendedProperties();
    assertTrue(properties.size() > 0);
    System.out.println(properties);
  }

  @Test
  public void testGetBuid() throws OdpsException {
    Project p = odps.projects().get();
    assertTrue(p.getAllProperties().containsKey("buid"));
  }

  @Test
  public void testGetTunnelEndpoint() throws Exception {
    String server = odps.projects().get().getTunnelEndpoint();
    assertNotNull(server);
    System.out.println(server);

    try {
      odps.projects().get("not_exists").getTunnelEndpoint();
      assertNotNull("Expect exception when project not found", null);
    } catch (OdpsException e) {
      assertTrue(e.getMessage().contains("not found"));
    }
  }

  @Test
  public void testUnmarshal() throws Exception {
    ProjectModel model = SimpleXmlUtils.unmarshal(xml.getBytes("utf-8"), ProjectModel.class);

    assertEquals("odps_mingyou_test", model.name);
    assertEquals("managed", model.type);
    assertEquals("project copy from [odps_test_tunnel_project]", model.comment);
    assertEquals("AVAILABLE", model.state);
    assertEquals("group_200", model.projectGroupName);
    Cluster cluster = model.clusters.entries.get(0);
    assertEquals("AT-200N", cluster.name);
    assertEquals("1", cluster.quotaID);
    assertTrue(model.properties.containsKey("odps.security.ip.whitelist"));
    assertEquals("", model.properties.get("odps.security.ip.whitelist"));
    assertTrue(model.properties.containsKey("property2"));
    assertEquals("val", model.properties.get("property2"));
  }

  @Test
  public void testMarshal() throws Exception {
    ProjectModel model = new ProjectModel();
    model.name = "odps_mingyou_test";
    model.type = Project.ProjectType.managed.toString().toLowerCase();
    model.comment = "project copy from [odps_test_tunnel_project]";
    model.state = "AVAILABLE";
    model.projectGroupName = "group_200";
    model.clusters = new Clusters();
    model.clusters.entries.add(new Cluster("AT-200N", "1"));
    model.properties = new LinkedHashMap<String, String>();
    model.properties.put("odps.security.ip.whitelist", "");
    model.properties.put("property2", "val");

    String unmarshalled = SimpleXmlUtils.marshal(model);
    assertEquals(xml, unmarshalled);
  }
}
