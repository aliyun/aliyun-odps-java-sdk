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

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.lot.common.ArgumentNullException;
import com.aliyun.odps.lot.common.Language;
import com.aliyun.odps.lot.common.Reference;
import com.aliyun.odps.lot.common.ReferencedURI;
import com.aliyun.odps.lot.common.Resource;
import com.aliyun.odps.lot.common.Schema;

import apsara.odps.lot.DataSinkProtos;
import apsara.odps.lot.LanguageSinkProtos;
import apsara.odps.lot.Lot;

public class LanguageSink extends DataSink {

  static public class Output {

    private Reference column;
    private OdpsType type;

    public Output(Reference column, OdpsType type) {
      this.column = column;
      this.type = type;
    }

    public Reference getColumn() {
      return column;
    }

    public OdpsType getType() {
      return type;
    }
  }


  private String className;
  private List<Resource> resources;
  private List<ReferencedURI> referencedURIs;
  private Language language;
  private List<Output> output;
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

  public List<Output> getOutput() {
    return output;
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

  public LanguageSink(Language language, List<Resource> resources, String className,
                      List<ReferencedURI> referencedURIs, List<Output> output, int workerCount,
                      Map<String, String> properties) {

    if (className == null) {
      throw new ArgumentNullException("className");
    }
    if (output == null) {
      throw new ArgumentNullException("output");
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
    this.output = output;
    this.insCount = workerCount;

  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 0);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();

    DataSinkProtos.DataSink.Builder src = DataSinkProtos.DataSink.newBuilder();
    src.setId(getId());
    src.setParentId(getParents().get(0).getId());

    LanguageSinkProtos.LanguageSink.Builder ls = LanguageSinkProtos.LanguageSink.newBuilder();
    ls.setLanguage(language.toProtoBuf());
    for (Resource res : resources) {
      LanguageSinkProtos.LanguageSink.Resources.Builder
          rb =
          LanguageSinkProtos.LanguageSink.Resources.newBuilder();
      rb.setProject(res.getProject());
      rb.setResourceName(res.getName());
      ls.addResources(rb.build());
    }
    ls.setClassName(className);
    for (ReferencedURI uri : referencedURIs) {
      ls.addURIs(uri.toProtoBuf());
    }
    for (Output col : output) {
      LanguageSinkProtos.LanguageSink.Output.Builder
          ob =
          LanguageSinkProtos.LanguageSink.Output.newBuilder();
      ob.setColumn(col.getColumn().toProtoBuf().getReference());
      ob.setType(Schema.Column.castType(col.getType()));
      ls.addOutput(ob.build());
    }
    ls.setInstanceCount(insCount);
    for (Map.Entry<String, String> prop : properties.entrySet()) {
      LanguageSinkProtos.LanguageSink.Properties.Builder
          pb =
          LanguageSinkProtos.LanguageSink.Properties.newBuilder();
      pb.setKey(prop.getKey());
      pb.setValue(prop.getValue());
      ls.addProperties(pb.build());
    }

    src.setLanguageSink(ls.build());
    builder.setDataSink(src.build());

    return builder.build();
  }
}
