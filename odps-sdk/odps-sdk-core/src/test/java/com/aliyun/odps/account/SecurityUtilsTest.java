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

package com.aliyun.odps.account;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Request.Method;

public class SecurityUtilsTest {

  @Test
  public void testBuildCanonicalizedResource() {
    String resource = "/projects/odps_test_tunnel_project/instances";
    String prefix = "x-odps-";
    Request request = new Request();
    request.setMethod(Method.POST);
    request.setHeader("x-odps-user-agent", "JavaSDK/0.12.0;Linux");
    request.setHeader("Date", "Tue, 13 May 2014 09:22:20 GMT");
    request.setHeader("Content-Length", "369");
    request.setHeader("Content-MD5", "746a0402967096663daa8bd1a2ff5c7c");
    request.setHeader("User-Agent", "JavaSDK/0.12.0;Linux");
    request.setHeader("Content-Type", "application/xml");
    request.setBodyLength(0L);

    String expectedResult = "POST\n746a0402967096663daa8bd1a2ff5c7c\napplication/xml\n"
                            + "Tue, 13 May 2014 09:22:20 GMT\nx-odps-user-agent:JavaSDK/0.12.0;Linux\n"
                            + "/projects/odps_test_tunnel_project/instances";

    String result = SecurityUtils.buildCanonicalString(resource, request, prefix);

    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public void testHmacsha1Signature() {

    int[] a = {
        80, 79, 83, 84, 10, 54, 99, 50, 101, 49, 48, 102, 49, 55, 98, 98, 55, 51, 56, 53,
        102, 50, 97, 99, 51, 48, 56, 49, 49, 98, 53, 101, 56, 101, 52, 54, 102, 10, 97, 112, 112,
        108, 105, 99, 97, 116, 105, 111, 110, 47, 120, 109, 108, 10, 84, 117, 101, 44, 32, 49, 51,
        32, 77, 97, 121, 32, 50, 48, 49, 52, 32, 48, 57, 58, 53, 49, 58, 49, 55, 32, 71, 77, 84,
        10, 120, 45, 111, 100, 112, 115, 45, 117, 115, 101, 114, 45, 97, 103, 101, 110, 116, 58,
        74, 97, 118, 97, 83, 68, 75, 47, 48, 46, 49, 50, 46, 48, 59, 76, 105, 110, 117, 120, 10,
        47, 112, 114, 111, 106, 101, 99, 116, 115, 47, 111, 100, 112, 115, 95, 116, 101, 115, 116,
        95, 116, 117, 110, 110, 101, 108, 95, 112, 114, 111, 106, 101, 99, 116, 47, 105, 110, 115,
        116, 97, 110, 99, 101, 115};
    byte[] data = fromIntToByte(a);
    int[] b = {111, 82, 100, 51, 48, 122, 55, 115, 86, 52, 104, 66, 88, 57, 97, 89, 116, 74, 103,
               105, 105, 53, 113, 110, 121, 104, 103, 61};
    byte[] key = fromIntToByte(b);
    int[] c = {-5, 103, 5, -52, 28, -21, -38, -2, -26, 2, 83, -107, 26, 11, -91, 67, -18, 124,
               -98, -28};
    byte[] expectedResult = fromIntToByte(c);
    byte[] result = SecurityUtils.hmacsha1Signature(data, key);
    Assert.assertEquals(true, isEquale(expectedResult, result));
  }

  private byte[] fromIntToByte(int[] data) {
    byte[] result = new byte[data.length];
    for (int i = 0; i < data.length; i++) {
      result[i] = (byte) data[i];
    }
    return result;
  }

  private boolean isEquale(byte[] l, byte[] r) {
    if (l == null) {
      return (r == null);
    }
    if (r == null) {
      return (l == null);
    }
    if (l.length != r.length) {
      return false;
    }
    for (int i = 0; i < l.length; i++) {
      if (l[i] != r[i]) {
        return false;
      }
    }
    return true;

  }
}
