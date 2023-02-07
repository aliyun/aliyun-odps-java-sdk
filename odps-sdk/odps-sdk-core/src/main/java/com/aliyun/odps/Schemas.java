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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.odps.task.SQLTask;

public class Schemas implements Iterable<Schema> {

  private RestClient client;
  private Odps odps;


  Schemas(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  public boolean exists(String schemaName) {
    return exists(odps.getDefaultProject(), schemaName);
  }

  public boolean exists(String projectName, String schemaName) {
    try {
      Iterator<Function> iter = odps.functions().iterator(projectName, schemaName);
      iter.hasNext();
      return true;
    } catch (RuntimeException e) {
      return false;
    }
  }

  @Override
  public Iterator<Schema> iterator() {
    return iterator(odps.getDefaultProject());
  }


  public Iterator<Schema> iterator(final String projectName) {
    return new SchemaListIterator(projectName);
  }

  public Iterable<Schema> iterable() {
    return iterable(odps.getDefaultProject());
  }

  public Iterable<Schema> iterable(final String projectName) {
    return () -> new SchemaListIterator(projectName);
  }

  private class SchemaListIterator extends ListIterator<Schema> {
    String projectName;
    boolean hasList = false;

    public SchemaListIterator(final String projectName) {
      if (StringUtils.isNullOrEmpty(projectName)) {
        throw new IllegalArgumentException("Argument 'projectName' cannot be null or empty");
      }

      this.projectName = projectName;
    }

    @Override
    protected List<Schema> list() {
      if (hasList) {
        return null;
      }
      ArrayList<Schema> schemas = new ArrayList<>();
      try {
        Instance i = SQLTask.run(odps, "show schemas in " + projectName + ";" );
        i.waitForSuccess();
        String[] schemaNames = i.getTaskResults().get("AnonymousSQLTask").split("\n");
        for (String name : schemaNames) {
          schemas.add(new Schema(name));
        }
      } catch (OdpsException e) {
        return null;
      }
      hasList = true;
      return schemas;
    }
  }
}
