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

package com.aliyun.odps.rest;

import java.io.InputStream;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.commons.transport.Response;

public class RestMockTest extends RestClient {

  public static class MockResponse extends Response {

    public MockResponse() {
      this.status = 200;
    }
  }

  private long retryTimes = 0;
  private long sleepTime = 0;

  public RestMockTest() {
    super(null);
    super.setConnectTimeout(2);
    super.setReadTimeout(2);
  }

  @Override
  protected Response requestWithNoRetry(String resource, String method, Map<String, String> params,
                                        Map<String, String> headers, InputStream body, long bodyLen)
      throws OdpsException {
    if (retryTimes > 0) {
      if (sleepTime > 0) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
        }
      }
      --retryTimes;
      throw new OdpsException("Failed in retry test.");
    }
    MockResponse resp = new MockResponse();
    return resp;
  }

  @Test
  public void testRetry() throws OdpsException {
    retryTimes = 3;
    request("", "GET", null, null, null, 0);
  }

  @Test(expected = OdpsException.class)
  public void testRetryFail() throws OdpsException {
    retryTimes = 4;
    request("", "GET", null, null, null, 0);
  }

  @Test(expected = OdpsException.class)
  public void testRetryPut() throws OdpsException {
    retryTimes = 1;
    request("", "PUT", null, null, null, 0);
  }



  @Test(expected = RuntimeException.class)
  public void testRetryLogger() throws OdpsException {

    class TestLoggerClass extends RetryLogger {

      @Override
      public void onRetryLog(Throwable e, long retryCount, long retrySleepTime) {
        System.out.println(retryCount);
        System.out.println(retrySleepTime);
        throw new RuntimeException(e.getMessage(), e);
      }
    };

    TestLoggerClass logger = new TestLoggerClass();

    setRetryLogger(logger);

    retryTimes = 3;
    request("", "GET", null, null, null, 0);
  }

  @Test
  public void testTime() throws OdpsException {
    retryTimes = 1;
    sleepTime = (getConnectTimeout() + getReadTimeout()) * 1000;
    long startTime = System.currentTimeMillis();
    request("", "GET", null, null, null, 0);
    long endTime = System.currentTimeMillis();
    assert (endTime - startTime < sleepTime + 5000);
  }
}
