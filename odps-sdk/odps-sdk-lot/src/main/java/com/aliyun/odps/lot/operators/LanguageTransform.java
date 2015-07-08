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
import java.util.List;

import com.aliyun.odps.lot.common.ArgumentNullException;
import com.aliyun.odps.lot.common.Language;
import com.aliyun.odps.lot.common.ReferencedURI;
import com.aliyun.odps.lot.common.Resource;
import com.aliyun.odps.lot.common.ScalarExpression;
import com.aliyun.odps.lot.common.Schema;

import apsara.odps.lot.LanguageTransformProtos;
import apsara.odps.lot.Lot;
import apsara.odps.lot.TransformProtos;

public class LanguageTransform extends Transform {

  private String className;
  private List<ReferencedURI> referencedURIs;
  private Language language;

  public String getClassName() {
    return className;
  }

  public List<ReferencedURI> getReferencedURIs() {
    return referencedURIs;
  }

  public Language getLanguage() {
    return language;
  }

  public LanguageTransform(List<ScalarExpression> parameters, List<Resource> resources,
                           Schema schema,
                           String className, List<ReferencedURI> referencedURIs,
                           Language language) {
    super(parameters, resources, schema);

    if (className == null) {
      throw new ArgumentNullException("className");
    }

    if (referencedURIs == null) {
      this.referencedURIs = new ArrayList<ReferencedURI>();
    } else {
      this.referencedURIs = referencedURIs;
    }

    this.className = className;
    this.language = language;
  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 1);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();

    TransformProtos.Transform.Builder trans = TransformProtos.Transform.newBuilder();
    trans.setId(getId());
    trans.setParentId(getParents().get(0).getId());

    for (ScalarExpression param : getParameters()) {
      trans.addParameters(param.toProtoBuf());
    }

    for (Resource res : getResources()) {
      TransformProtos.Transform.Resources.Builder
          rb =
          TransformProtos.Transform.Resources.newBuilder();
      rb.setProject(res.getProject());
      rb.setResourceName(res.getName());
      trans.addResources(rb.build());
    }

    LanguageTransformProtos.LanguageTransform.Builder
        ll =
        LanguageTransformProtos.LanguageTransform.newBuilder();
    ll.setClassName(className);
    for (ReferencedURI uri : referencedURIs) {
      ll.addURIs(uri.toProtoBuf());
    }
    ll.setLanguage(language.toProtoBuf());

    trans.setLanguageTransform(ll.build());
    trans.setSchema(getSchema().toProtoBuf());

    builder.setTransform(trans.build());

    return builder.build();
  }
}
