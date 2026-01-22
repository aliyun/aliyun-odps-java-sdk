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

package com.aliyun.odps.data;

import java.util.Arrays;
import java.util.Objects;

public final class RawGeographyObject implements GeographyObject {

  private static final long serialVersionUID = 1L;

  private final byte[] binary;

  public RawGeographyObject(byte[] binary) {
    this.binary = Objects.requireNonNull(binary, "binary must not be null").clone();
  }

  @Override
  public byte[] asBinary() {
    return binary.clone();
  }

  @Override
  public String asText() {
    return null;
  }

  @Override
  public String toString() {
    return "RawGeographyObject{binaryLength=" + binary.length + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RawGeographyObject)) {
      return false;
    }
    RawGeographyObject that = (RawGeographyObject) o;
    return Arrays.equals(binary, that.binary);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(binary);
  }
}
