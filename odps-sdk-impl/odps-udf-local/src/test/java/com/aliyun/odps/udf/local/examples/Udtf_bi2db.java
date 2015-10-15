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

package com.aliyun.odps.udf.local.examples;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g., "d string,string->string,bigint".
@Resolve({"boolean,bigint->double,boolean"})
public class Udtf_bi2db extends UDTF {

  @Override
  public void process(Object[] args) throws UDFException {

    Boolean b = (args[0] == null) ? null : (Boolean) args[0];
    Long l = (args[1] == null) ? null : (Long) args[1];

    forward(b != null && b ? 1.1D : 0D,
            l != null && l > 0 ? new Boolean(true) : new Boolean(false));

  }
}