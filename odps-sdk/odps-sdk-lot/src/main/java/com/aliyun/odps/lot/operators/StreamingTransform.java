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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.lot.common.ArgumentNullException;
import com.aliyun.odps.lot.common.Resource;
import com.aliyun.odps.lot.common.ScalarExpression;
import com.aliyun.odps.lot.common.Schema;

import apsara.odps.lot.Lot;
import apsara.odps.lot.StreamingTransformProtos;
import apsara.odps.lot.TransformProtos;

public class StreamingTransform extends Transform {

  private String cmd;
  private Map<String, String> properties = new HashMap<String, String>();

  public StreamingTransform(List<ScalarExpression> parameters, List<Resource> resources,
                            Schema schema, String cmd) {
    super(parameters, resources, schema);

    if (cmd == null) {
      throw new ArgumentNullException("cmd");
    }

    this.cmd = cmd;
  }

  public String getCmd() {
    return cmd;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void addProperty(String key, String value) {
    if (key == null) {
      throw new ArgumentNullException("key");
    }

    if (value == null) {
      throw new ArgumentNullException("value");
    }

    properties.put(key, value);
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

    StreamingTransformProtos.StreamingTransform.Builder
        st =
        StreamingTransformProtos.StreamingTransform.newBuilder();
    st.setCmd(cmd);

    for (Map.Entry<String, String> prop : properties.entrySet()) {
      StreamingTransformProtos.StreamingTransform.Properties.Builder
          sb =
          StreamingTransformProtos.StreamingTransform.Properties.newBuilder();
      sb.setKey(prop.getKey());
      sb.setValue(prop.getValue());
      st.addProperties(sb.build());
    }

    trans.setStreamingTransform(st.build());
    trans.setSchema(getSchema().toProtoBuf());

    builder.setTransform(trans.build());

    return builder.build();
  }

}
