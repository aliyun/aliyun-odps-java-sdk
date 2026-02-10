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

package com.aliyun.odps.table.arrow.accessor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;

public class ArrowTimestampExtensionAccessor extends ArrowVectorAccessor{

  protected final BigIntVector sec;
  protected final IntVector nano;

  public ArrowTimestampExtensionAccessor(StructVector structVector) {
    super(structVector);
    sec = (BigIntVector)structVector.getVectorById(0);
    nano = (IntVector)structVector.getVectorById(1);
  }

  public LocalDateTime getTimestampNtz(int index) {
    return LocalDateTime.ofEpochSecond(sec.get(index), nano.get(index), ZoneOffset.UTC);
  }
  public Instant getTimestamp(int index) {
    return Instant.ofEpochSecond(sec.get(index), nano.get(index));
  }
}
