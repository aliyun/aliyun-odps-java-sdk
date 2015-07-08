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

package com.aliyun.odps.graph.common;

import com.aliyun.odps.graph.GRAPH_CONF;

/**
 * 定义 ODPS Graph 作业配置中local 和 graph公用的配置参数
 */
public class COMMON_GRAPH_CONF extends GRAPH_CONF {

  public final static String VERTEX_ID_CLASS = "odps.graph.vertex.id.class";
  public final static String VERTEX_VALUE_CLASS = "odps.graph.vertex.value.class";
  public final static String EDGE_VALUE_CLASS = "odps.graph.edge.value.class";
  public final static String MESSAGE_VALUE_CLASS = "odps.graph.message.value.class";

  public final static String
      JOB_MAX_USER_DEFINED_COUNTERS_NUM =
      "odps.graph.job.max.user.defined.counters.num";

  public final static String USE_NEW_SDK = "odps.graph.new.sdk";
}

