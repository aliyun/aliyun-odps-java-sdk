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

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.udf.local.util.TestUtils;

// TODO define input and output types, e.g., "d string,string->string,bigint".
@Resolve({"string,string->string,bigint,string"})
public class Udtf_ss2sis_Datetime_Resource extends UDTF {

  ExecutionContext ctx;
  String resourceContent = "";


  @Override
  public void setup(ExecutionContext ctx) throws UDFException {
    this.ctx = ctx;
    try {

      Iterator<Object[]> iterator = ctx.readResourceTable("table_resource3").iterator();
      while (iterator.hasNext()) {
        if (!resourceContent.equals("")) {
          resourceContent += "|";
        }
        Date date = (Date) iterator.next()[1];
        resourceContent += TestUtils.getDateFormat(TestUtils.DATE_FORMAT_2).format(date);
      }


    } catch (IOException e) {
      throw new UDFException(e);
    }
  }

  @Override
  public void process(Object[] args) throws UDFException {
    String a = (String) args[0];
    long b = args[1] == null ? 0 : ((String) args[1]).length();

    forward(a, b, resourceContent);

  }
}