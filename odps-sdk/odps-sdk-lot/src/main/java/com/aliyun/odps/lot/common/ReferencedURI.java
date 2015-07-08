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

package com.aliyun.odps.lot.common;

import apsara.odps.ReferencedURIProtos;

public class ReferencedURI {

  private String uri;
  private Permission permission;

  public String getUri() {
    return uri;
  }

  public Permission getPermission() {
    return permission;
  }

  public ReferencedURI(String uri, Permission permission) {
    if (uri == null) {
      throw new ArgumentNullException("uri");
    }

    this.uri = uri;
    this.permission = permission;
  }

  public ReferencedURIProtos.ReferencedURI toProtoBuf() {
    ReferencedURIProtos.ReferencedURI.Builder
        builder =
        ReferencedURIProtos.ReferencedURI.newBuilder();
    builder.setURI(uri);
    builder.setPermission(permission.toProtoBuf());

    return builder.build();
  }
}
