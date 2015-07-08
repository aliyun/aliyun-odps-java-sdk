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

package com.aliyun.odps.lot.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.lot.common.ArgumentNullException;
import com.aliyun.odps.lot.common.Language;
import com.aliyun.odps.lot.common.ReferencedURI;
import com.aliyun.odps.lot.common.Resource;
import com.aliyun.odps.lot.common.Schema;

import apsara.odps.lot.DataSourceProtos;
import apsara.odps.lot.LanguageSourceProtos;
import apsara.odps.lot.Lot;

public class LanguageSource extends DataSource {

  private String className;
  private List<Resource> resources;
  private List<ReferencedURI> referencedURIs;
  private Language language;
  private Schema schema;
  private int insCount;
  private Map<String, String> properties = new HashMap<String, String>();

  public String getClassName() {
    return className;
  }

  public List<ReferencedURI> getReferencedURIs() {
    return referencedURIs;
  }

  public Language getLanguage() {
    return language;
  }

  public List<Resource> getResources() {
    return resources;
  }

  public Schema getSchema() {
    return schema;
  }

  public int getWorkerCount() {
    return insCount;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void addProperty(String key, String value) {
    properties.put(key, value);
  }

  public LanguageSource(Language language, List<Resource> resources, String className,
                        List<ReferencedURI> referencedURIs, Schema schema, int workerCount,
                        Map<String, String> properties) {

    if (className == null) {
      throw new ArgumentNullException("className");
    }
    if (schema.getColumns().size() == 0) {
      throw new ArgumentNullException("schema");
    }
    if (workerCount <= 0) {
      throw new ArgumentNullException("workerCount");
    }

    if (resources == null) {
      this.resources = new ArrayList<Resource>();
    } else {
      this.resources = resources;
    }
    if (referencedURIs == null) {
      this.referencedURIs = new ArrayList<ReferencedURI>();
    } else {
      this.referencedURIs = referencedURIs;
    }
    if (properties == null) {
      this.properties = new HashMap<String, String>();
    } else {
      this.properties = properties;
    }

    this.className = className;
    this.language = language;
    this.schema = schema;
    this.insCount = workerCount;

  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 0);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();

    DataSourceProtos.DataSource.Builder src = DataSourceProtos.DataSource.newBuilder();
    src.setId(getId());

    LanguageSourceProtos.LanguageSource.Builder
        ls =
        LanguageSourceProtos.LanguageSource.newBuilder();
    ls.setLanguage(language.toProtoBuf());
    for (Resource res : resources) {
      LanguageSourceProtos.LanguageSource.Resources.Builder
          rb =
          LanguageSourceProtos.LanguageSource.Resources.newBuilder();
      rb.setProject(res.getProject());
      rb.setResourceName(res.getName());
      ls.addResources(rb.build());
    }
    ls.setClassName(className);
    for (ReferencedURI uri : referencedURIs) {
      ls.addURIs(uri.toProtoBuf());
    }
    ls.setSchema(getSchema().toProtoBuf());
    ls.setInstanceCount(insCount);
    for (Map.Entry<String, String> prop : properties.entrySet()) {
      LanguageSourceProtos.LanguageSource.Properties.Builder
          pb =
          LanguageSourceProtos.LanguageSource.Properties.newBuilder();
      pb.setKey(prop.getKey());
      pb.setValue(prop.getValue());
      ls.addProperties(pb.build());
    }

    src.setLanguageSource(ls.build());
    builder.setDataSource(src.build());

    return builder.build();
  }
}
