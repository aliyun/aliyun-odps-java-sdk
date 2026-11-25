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

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Response;
import org.junit.Test;

import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Who owns the request id of a failed REST call.
 *
 * <p>Runs against an in-process stub transport: no network, no cloud credentials. Kept separate
 * from {@code RestMockTest} so it stays cheap enough to run in the CI test whitelist.
 */
public class RestErrorResponseRequestIdTest extends RestClient {

    private static class StubResponse extends Response {
        StubResponse(int status, byte[] body, String requestId) {
            this.status = status;
            this.body = body;
            this.headers.put(Headers.ODPS_REQUEST_ID, requestId);
        }
    }

    private Response nextResponse;

    public RestErrorResponseRequestIdTest() {
        super(null);
    }

    @Override
    protected Response requestWithNoRetry(String resource, String method, Map<String, String> params,
                                          Map<String, String> headers, InputStream body, long bodyLen)
            throws OdpsException {
        Response response = nextResponse;
        nextResponse = null;
        return response;
    }

    private OdpsException failureOf(int status, String body, String headerRequestId) {
        nextResponse = new StubResponse(status, body == null ? new byte[0] : body.getBytes(),
                headerRequestId);
        try {
            request("", "POST", null, null, null, 0);
        } catch (OdpsException e) {
            return e;
        }
        fail("expected the failure response to be raised");
        return null;
    }

    /**
     * A gateway or proxy failure body is not an ODPS error document, so the request id only exists
     * in the response header. Dropping it left callers with a status and nothing to search for.
     */
    @Test
    public void emptyFailureBodyStillCarriesTheHeaderRequestId() {
        OdpsException failure = failureOf(502, "", "gateway-req-1");
        assertEquals(Integer.valueOf(502), failure.getStatus());
        assertEquals("gateway-req-1", failure.getRequestId());
    }

    /** The service's own error document stays authoritative; the header must not overwrite it. */
    @Test
    public void odpsErrorBodyRequestIdIsNotOverwritten() {
        String body = "{\"RequestId\":\"body-req-1\",\"Code\":\"NoPermission\",\"Message\":\"nope\"}";
        OdpsException failure = failureOf(403, body, "header-req-2");
        assertEquals(Integer.valueOf(403), failure.getStatus());
        assertEquals("body-req-1", failure.getRequestId());
    }

    @Test
    public void emptyHeaderDoesNotBecomeAnEmptyRequestId() {
        OdpsException failure = failureOf(502, "", "");
        assertNull(failure.getRequestId());
    }
}
