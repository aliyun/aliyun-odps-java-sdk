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

package com.aliyun.odps;

import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

/** Verify request counts and complete payloads without making network calls. */
public class ResourceUploadBoundaryTest {
  static class Request {
    String method; Map<String, String> params; byte[] body;
  }
  static class CapturingClient extends RestClient {
    List<Request> requests = new ArrayList<>();
    boolean requestStarted;
    CapturingClient() { super(null); }
    @Override public Response request(String path, String method, Map<String, String> params,
        Map<String, String> headers, InputStream body, long length) {
      try {
        requestStarted = true;
        Request request = new Request(); request.method = method; request.params = params;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int value; while ((value = body.read()) != -1) { bytes.write(value); }
        request.body = bytes.toByteArray(); assertEquals(length, request.body.length);
        requests.add(request); return null;
      } catch (Exception e) { throw new AssertionError(e); }
    }
  }
  private CapturingClient upload(byte[] bytes, boolean update, boolean shortReads) throws Exception {
    final CapturingClient capturedClient = new CapturingClient();
    Odps odps = new Odps(new AliyunAccount("unused", "unused")) {
      @Override public RestClient getRestClient() { return capturedClient; }
    };
    Resources resources = new Resources(odps); resources.setChunkSize(4);
    FileResource resource = new FileResource(); resource.setName("offline-resource");
    InputStream input = new ByteArrayInputStream(bytes) {
      @Override public synchronized int read(byte[] b, int off, int len) {
        return super.read(b, off, shortReads ? Math.min(len, 1) : len);
      }
    };
    if (update) { resources.update("offline", "schema", resource, input); }
    else { resources.create("offline", "schema", resource, input); }
    return capturedClient;
  }
  @Test public void smallEmptyAndExactThresholdUseOneRequest() throws Exception {
    for (int size : new int[]{0, 1, 3, 4}) {
      byte[] bytes = new byte[size];
      for (int i = 0; i < size; i++) { bytes[i] = (byte) (i + 1); }
      for (boolean update : new boolean[]{false, true}) {
        CapturingClient client = upload(bytes, update, true);
        assertEquals(1, client.requests.size());
        assertEquals(update ? "PUT" : "POST", client.requests.get(0).method);
        assertArrayEquals(bytes, client.requests.get(0).body);
        assertFalse(client.requests.get(0).params.containsKey("rOpMerge"));
      }
    }
  }
  @Test public void largeAndShortReadStreamsPreserveAllData() throws Exception {
    byte[] bytes = new byte[]{1,2,3,4,5,6,7,8,9};
    for (boolean shortReads : new boolean[]{false, true}) {
      CapturingClient client = upload(bytes, false, shortReads);
      assertTrue(client.requests.size() >= 4);
      ByteArrayOutputStream content = new ByteArrayOutputStream();
      for (int i = 0; i < client.requests.size() - 1; i++) {
        content.write(client.requests.get(i).body);
      }
      assertArrayEquals(bytes, content.toByteArray());
      assertEquals("true", client.requests.get(client.requests.size() - 1).params.get("rOpMerge"));
    }
  }

  @Test public void unknownLengthStreamStartsUploadBeforeWaitingForEof() throws Exception {
    CapturingClient capturedClient = new CapturingClient();
    Odps odps = new Odps(new AliyunAccount("unused", "unused")) {
      @Override public RestClient getRestClient() { return capturedClient; }
    };
    Resources resources = new Resources(odps);
    resources.setChunkSize(4);
    FileResource resource = new FileResource();
    resource.setName("streamed-resource");
    InputStream input = new InputStream() {
      private boolean emitted;

      @Override public int read(byte[] bytes, int offset, int length) throws IOException {
        if (!emitted) {
          bytes[offset] = 7;
          emitted = true;
          return 1;
        }
        if (!capturedClient.requestStarted) {
          throw new IOException("waited for more input before starting the first request");
        }
        return -1;
      }

      @Override public int read() throws IOException {
        byte[] one = new byte[1];
        return read(one, 0, 1) == -1 ? -1 : one[0] & 0xff;
      }
    };

    resources.create("offline", "schema", resource, input);

    assertEquals(2, capturedClient.requests.size());
    assertArrayEquals(new byte[] {7}, capturedClient.requests.get(0).body);
    assertEquals("true", capturedClient.requests.get(1).params.get("rOpMerge"));
  }
}
