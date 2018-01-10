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

import java.math.BigDecimal;
import java.util.Date;

import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.local.util.TestUtils;

public class UdfExample extends UDF {

  public String evaluate(String a, String b) {
    return "ss2s:" + a + "," + b;
  }

  public String evaluate(String a, String b, String c, String d) {
    return "ssss2s:" + a + "," + b + "," + c + "," + d;
  }

  public String evaluate(String a, String b, String c) {
    return "sss2s:" + a + "," + b + "," + c;
  }

  public String evaluate(String a, Long b) {

    return "sss2s:" + a + "|" + TestUtils.getDateFormat(TestUtils.DATE_FORMAT_2).format(new Date(b));
  }

  public Long evaluate(Long a, Long b) {

    return a + b;
  }

  public Double evaluate(Double a, Double b) {

    return a + b;
  }

  public Boolean evaluate(Boolean a, Boolean b) {
    if (a == null || b == null) {
      return false;
    }
    return a && b;

  }

  public BigDecimal evaluate(BigDecimal... arr) {
    if (arr == null || arr.length == 0) {
      return null;
    }
    BigDecimal result = new BigDecimal(0);
    for(BigDecimal tmp : arr) {
      if (tmp != null) {
        result = result.add(tmp);
      }
    }
    return result;
  }

  // negative
  public static String evaluate(String a) {
    return a;
  }

}
