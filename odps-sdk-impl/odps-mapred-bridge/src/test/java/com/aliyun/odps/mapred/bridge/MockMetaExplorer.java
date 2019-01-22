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

package com.aliyun.odps.mapred.bridge;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Resource.Type;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Task;

public class MockMetaExplorer implements MetaExplorer {

  @Override
  public Table getTable(String proj, String tblName) {
    Table mt = mock(Table.class);

    when(mt.getSchema()).thenAnswer(new Answer<TableSchema>() {

      @Override
      public TableSchema answer(InvocationOnMock invocation) throws Throwable {
        TableSchema ts = new TableSchema();
        ts.addColumn(new Column("key", OdpsType.STRING));
        ts.addColumn(new Column("value", OdpsType.BIGINT));
        return ts;
      }

    });

    return mt;
  }

  @Override
  public boolean existsTable(String proj, String name) throws OdpsException {
    return true;
  }

  @Override
  public String addFileResourceWithRetry(String filePath, Type type, String padding,
                                         boolean isTempResource) throws OdpsException {
    return FilenameUtils.getBaseName(filePath) + padding + FilenameUtils.getExtension(filePath);
  }

  @Override
  public String getDefaultProject() {
    return "foo_proj";
  }

  @Override
  public void deleteResource(String name) throws OdpsException {

  }

  @Override
  public boolean existsResource(String proj, String name) throws OdpsException {
    return true;
  }

  public void deleteFunction(String project, String name) throws OdpsException {
  }

  @Override
  public void createFunction(List<String> resources, String project, String name, String className)
      throws OdpsException {
  }

  @Override
  public Instance createInstance(Task task, int priority) {
    return null;
  }

  @Override
  public String addTempResourceWithRetry(InputStream in, String name, Type type)
      throws OdpsException {
    return name;
  }

  @Override
  public boolean existsVolume(String projectName, String volumeName)
      throws OdpsException {
    return true;
  }

  public String getProjectProperty(String key) throws OdpsException{
    return null;
  }
}
