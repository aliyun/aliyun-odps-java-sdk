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

package com.aliyun.odps.rest;

import org.junit.Assert;
import org.junit.Test;

public class ResourceBuilderTest {

  @Test
  public void testBuildProjectsResource() {
    Assert.assertEquals("/projects", ResourceBuilder.buildProjectsResource());
  }

  @Test
  public void testBuildProjectResource() {
    Assert.assertEquals("/projects/project_Name_01",
                        ResourceBuilder.buildProjectResource("project_Name_01"));
    Assert.assertEquals("/projects/project_%3F_01",
                        ResourceBuilder.buildProjectResource("project_?_01"));
    Assert.assertEquals("/projects/project_%26_01",
                        ResourceBuilder.buildProjectResource("project_&_01"));
    Assert.assertEquals("/projects/project_%25_01",
                        ResourceBuilder.buildProjectResource("project_%_01"));
    Assert.assertEquals("/projects/project_%23_01",
                        ResourceBuilder.buildProjectResource("project_#_01"));
  }

  @Test
  public void testBuildTablesResource() {
    Assert.assertEquals("/projects/projectName/tables",
                        ResourceBuilder.buildTablesResource("projectName"));
  }

  @Test
  public void testBuildTableResource() {
    Assert.assertEquals("/projects/projectName/tables/table_name_01",
                        ResourceBuilder.buildTableResource("projectName", "table_name_01"));
    Assert.assertEquals("/projects/projectName/tables/table_%3F_01",
                        ResourceBuilder.buildTableResource("projectName", "table_?_01"));
    Assert.assertEquals("/projects/projectName/tables/table_%26_01",
                        ResourceBuilder.buildTableResource("projectName", "table_&_01"));
    Assert.assertEquals("/projects/projectName/tables/table_%25_01",
                        ResourceBuilder.buildTableResource("projectName", "table_%_01"));
    Assert.assertEquals("/projects/projectName/tables/table_%23_01",
                        ResourceBuilder.buildTableResource("projectName", "table_#_01"));
    Assert.assertEquals("/projects/projectName/tables/table_%20_%0901",
                        ResourceBuilder.buildTableResource("projectName", "table_ _\t01"));
  }

  @Test
  public void testBuildFunctionsResource() {
    Assert.assertEquals("/projects/projectName/registration/functions",
                        ResourceBuilder.buildFunctionsResource("projectName"));
  }

  @Test
  public void testBuildFunctionResource() {
    Assert.assertEquals("/projects/projectName/registration/functions/function_name_01",
                        ResourceBuilder.buildFunctionResource("projectName", "function_name_01"));
  }

  @Test
  public void testBuildInstancesResource() {
    Assert.assertEquals("/projects/projectName/instances",
                        ResourceBuilder.buildInstancesResource("projectName"));
  }

  @Test
  public void testBuildInstanceResource() {
    Assert.assertEquals("/projects/projectName/instances/20140408082500459gggvs1f9", ResourceBuilder
        .buildInstanceResource("projectName", "20140408082500459gggvs1f9"));
  }

  @Test
  public void testBuildResourcesResource() {
    Assert.assertEquals("/projects/projectName/resources",
                        ResourceBuilder.buildResourcesResource("projectName"));
  }

  @Test
  public void testBuildResourceResource() {
    Assert.assertEquals("/projects/projectName/resources/a_b-c.d.jar",
                        ResourceBuilder.buildResourceResource("projectName", "a_b-c.d.jar"));
  }

}
