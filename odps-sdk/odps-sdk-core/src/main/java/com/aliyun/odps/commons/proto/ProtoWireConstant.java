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

package com.aliyun.odps.commons.proto;

/**
 * This class contains constants useful for dealing with
 * the Protocol Buffer wire format.
 *
 * @author kenton@google.com Kenton Varda
 */
class ProtoWireConstant {
  public static final int TUNNEL_META_COUNT = 33554430; // magic num 2^25-2
  public static final int TUNNEL_META_CHECKSUM = 33554431; // magic num 2^25-1
  public static final int TUNNEL_END_RECORD = 33553408; // maigc num 2^25-1024
  public static final int SCHEMA_END_TAG = 33553920; //maigc num 2^25-512

  public static final int METRICS_TAG = 1;
  public static final int METRICS_END_TAG = 33554176; // magic num 2^25-256
}
