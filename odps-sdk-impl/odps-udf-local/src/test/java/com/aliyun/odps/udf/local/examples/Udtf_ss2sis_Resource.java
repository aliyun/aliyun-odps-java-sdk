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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g., "d string,string->string,bigint".
@Resolve({"string,string->string,bigint,string"})
public class Udtf_ss2sis_Resource extends UDTF {

  ExecutionContext ctx;
  long fileResourceLineCount;
  long tableResource1RecordCount;
  long tableResource2RecordCount;

  @Override
  public void setup(ExecutionContext ctx) throws UDFException {
    this.ctx = ctx;
    try {
      InputStream in = ctx.readResourceFileAsStream("file_resource.txt");
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      fileResourceLineCount = 0;
      while ((line = br.readLine()) != null) {
        fileResourceLineCount++;
      }
      br.close();

      Iterator<Object[]> iterator = ctx.readResourceTable("table_resource1").iterator();
      tableResource1RecordCount = 0;
      while (iterator.hasNext()) {
        tableResource1RecordCount++;
        iterator.next();
      }

      iterator = ctx.readResourceTable("table_resource2").iterator();
      tableResource2RecordCount = 0;
      while (iterator.hasNext()) {
        tableResource2RecordCount++;
        iterator.next();
      }

    } catch (IOException e) {
      throw new UDFException(e);
    }
  }

  @Override
  public void process(Object[] args) throws UDFException {
    String a = (String) args[0];
    long b = args[1] == null ? 0 : ((String) args[1]).length();

    forward(a, b, "fileResourceLineCount=" + fileResourceLineCount + "|tableResource1RecordCount="
                  + tableResource1RecordCount + "|tableResource2RecordCount="
                  + tableResource2RecordCount);

  }
}