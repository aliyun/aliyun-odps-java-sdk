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
import static org.junit.Assert.fail;

import com.aliyun.odps.Project.ProjectModel;
import com.aliyun.odps.Projects.ListProjectResponse;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.rest.SimpleXmlUtils;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class ProjectsTest extends TestBase {
  private String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<Projects>\n"
      + "   <Project>\n"
      + "      <Name>a_0220210335__387</Name>\n"
      + "      <Type>managed</Type>\n"
      + "      <Comment>test replication prj</Comment>\n"
      + "      <Owner>ALIYUN$odpstest5@aliyun-inc.com</Owner>\n"
      + "      <State>AVAILABLE</State>\n"
      + "      <LastModifiedTime>Tue, 20 Feb 2018 13:03:35 GMT</LastModifiedTime>\n"
      + "      <CreationTime>Tue, 20 Feb 2018 13:03:35 GMT</CreationTime>\n"
      + "   </Project>\n"
      + "   <Project>\n"
      + "      <Name>a_0220210337_test3_239</Name>\n"
      + "      <Type>external</Type>\n"
      + "      <Comment>test replication prj</Comment>\n"
      + "      <Owner>ALIYUN$odpstest5@aliyun-inc.com</Owner>\n"
      + "      <State>AVAILABLE</State>\n"
      + "      <LastModifiedTime>Tue, 20 Feb 2018 13:03:37 GMT</LastModifiedTime>\n"
      + "      <CreationTime>Tue, 20 Feb 2018 13:03:37 GMT</CreationTime>\n"
      + "   </Project>\n"
      + "</Projects>";

  @Test
  public void testExistsString() throws OdpsException {
    boolean flag = odps.projects().exists("projectName");
    assertFalse(flag);
  }

  @Test
  public void testGet() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    Project prj = odps.projects().get();
    prj.getLastModifiedTime();

    // System.out.println("Name: " + prj.getName());
    // System.out.println("Comment: " + prj.getComment());
    // System.out.println("Owner: " + prj.getOwner());
    // System.out.println("CreatedTime: " + prj.getCreatedTime());
    // System.out.println("LastModifiedTime: " + prj.getLastModifiedTime());
    // System.out.println("Properties: " + prj.getProperties());
    //
    // Assert.assertEquals(odps.getDefaultProject(), prj.getName());
    // Assert.assertNotNull(prj.getOwner());
    // Assert.assertNotNull(prj.getComment());
    // Assert.assertNotNull(prj.getCreatedTime());
    // Assert.assertNotNull(prj.getLastModifiedTime());
    // Assert.assertNotNull(prj.getProperties());
  }

  @Test
  public void testGetString() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    Project prj = odps.projects().get(odps.getDefaultProject());
    Assert.assertNotNull(prj.getOwner());
  }

  @Test
  public void testUnmarshal() throws Exception {
    ListProjectResponse response = SimpleXmlUtils
        .unmarshal(xml.getBytes(), ListProjectResponse.class);
    List<ProjectModel> projects = response.projects;

    ProjectModel firstProject = projects.get(0);
    assertEquals("a_0220210335__387", firstProject.name);
    assertEquals("managed", firstProject.type);
    assertEquals("test replication prj", firstProject.comment);
    assertEquals("ALIYUN$odpstest5@aliyun-inc.com", firstProject.owner);
    assertEquals("AVAILABLE", firstProject.state);

    ProjectModel secondProject = projects.get(1);
    assertEquals("a_0220210337_test3_239", secondProject.name);
    assertEquals("external", secondProject.type);
    assertEquals("test replication prj", secondProject.comment);
    assertEquals("ALIYUN$odpstest5@aliyun-inc.com", secondProject.owner);
    assertEquals("AVAILABLE", secondProject.state);
  }

  @Test
  public void testUpdateProject() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    Project project = odps.projects().get();
    Map<String, String> properties = odps.projects().get().getProperties();
    String oldValue = properties.get("READ_TABLE_MAX_ROW");

    try {
      String newValue;
      if (oldValue == null) {
        newValue = "10000";
      } else if ("9999".equals(oldValue)) {
        newValue = "10000";
      } else {
        newValue = "9999";
      }
      properties.put("READ_TABLE_MAX_ROW", newValue);
      odps.projects().updateProject(project.getName(), properties);
      project.reload();
      assertEquals(newValue, project.getProperties().get("READ_TABLE_MAX_ROW"));
    } finally {
      // Restore READ_TABLE_MAX_ROW to 10000
      if (oldValue == null || "9999".equals(oldValue)) {
        properties.put("READ_TABLE_MAX_ROW", "10000");
        odps.projects().updateProject(project.getName(), properties);
        project.reload();
        assertEquals("10000", project.getProperties().get("READ_TABLE_MAX_ROW"));
      }
    }
  }

  @Test
  public void testListProjectsIterable() {
    String projectOwner = "ALIYUN$odpstest1@aliyun.com";
    Odps odps = OdpsTestUtils.newDefaultOdps();
    for(Project project : odps.projects().iterable(projectOwner)) {
      // 1365937150772213 is the internal format of project owner.
      if (!project.getOwner().equals(projectOwner) &&
          !project.getOwner().contains("1365937150772213")) {
        fail("Found project not belong to the owner");
      }
    }
  }

  @Test
  public void testListProjectsIterator() {
    String projectOwner = "ALIYUN$odpstest1@aliyun.com";
    Odps odps = OdpsTestUtils.newDefaultOdps();
    Iterator<Project> iterator = odps.projects().iterator(projectOwner);
    while (iterator.hasNext()) {
      Project project = iterator.next();
      // 1365937150772213 is the internal format of project owner.
      if (!project.getOwner().equals(projectOwner) &&
          !project.getOwner().contains("1365937150772213")) {
        fail("Found project not belong to the owner");
      }
    }
  }
}
