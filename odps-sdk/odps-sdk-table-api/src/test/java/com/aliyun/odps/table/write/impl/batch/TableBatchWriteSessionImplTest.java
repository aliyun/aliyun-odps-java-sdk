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

package com.aliyun.odps.table.write.impl.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.retry.RetryHeaders;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;

public class TableBatchWriteSessionImplTest {

    private final WriterCommitMessage[] messages = {
            new WriterCommitMessageImpl(0, "commit-message")
    };

    @Test
    public void testLegacyCommitRequestOmitsWaitTimeout() {
        JsonObject request = new JsonParser().parse(
                TableBatchWriteSessionImpl.generateCommitRequest(messages, null))
                .getAsJsonObject();

        Assert.assertEquals(1, request.getAsJsonArray("CommitMessages").size());
        Assert.assertFalse(request.has("WaitFlyingWritersTimeoutSeconds"));
    }

    @Test
    public void testCommitRequestIncludesWaitTimeoutBounds() {
        JsonObject lowerBound = new JsonParser().parse(
                TableBatchWriteSessionImpl.generateCommitRequest(messages, 1))
                .getAsJsonObject();
        JsonObject upperBound = new JsonParser().parse(
                TableBatchWriteSessionImpl.generateCommitRequest(messages, 600))
                .getAsJsonObject();

        Assert.assertEquals(
                1, lowerBound.get("WaitFlyingWritersTimeoutSeconds").getAsInt());
        Assert.assertEquals(
                600, upperBound.get("WaitFlyingWritersTimeoutSeconds").getAsInt());
    }

    @Test
    public void testCommitRequestSendsOutOfRangeWaitTimeout() {
        assertOutOfRangeWaitTimeoutSent(0);
        assertOutOfRangeWaitTimeoutSent(601);
    }

    @Test
    public void testTableRequestHeadersAreIsolatedAcrossRetries() throws Exception {
        TunnelRetryHandler retryHandler = new TunnelRetryHandler(
                new TunnelRetryHandler.RetryPolicy() {
                    @Override
                    public boolean shouldRetry(Exception e, int attempt) {
                        return attempt == 1;
                    }

                    @Override
                    public long getRetryWaitTime(int attempt) {
                        return 0;
                    }

                    @Override
                    public void waitForNextRetry(int attempt) {
                        // Keep this unit test independent of real retry backoff.
                    }
                }, null);
        Map<String, String> baseHeaders = new HashMap<>();
        baseHeaders.put("existing-header", "existing-value");
        List<Map<String, String>> attemptHeaders = new ArrayList<>();

        retryHandler.executeWithRetry(ctx -> {
            Map<String, String> requestHeaders = new HashMap<>(baseHeaders);
            ctx.injectHeaders(requestHeaders);
            attemptHeaders.add(requestHeaders);
            if (attemptHeaders.size() == 1) {
                throw new IOException("retry once");
            }
            return null;
        });

        Assert.assertEquals(2, attemptHeaders.size());
        Assert.assertFalse(baseHeaders.containsKey(RetryHeaders.TRACE_ID));
        Assert.assertFalse(baseHeaders.containsKey(RetryHeaders.RETRY_INDEX));
        Assert.assertEquals(
                attemptHeaders.get(0).get(RetryHeaders.TRACE_ID),
                attemptHeaders.get(1).get(RetryHeaders.TRACE_ID));
        Assert.assertEquals("0", attemptHeaders.get(0).get(RetryHeaders.RETRY_INDEX));
        Assert.assertEquals("1", attemptHeaders.get(1).get(RetryHeaders.RETRY_INDEX));
        Assert.assertEquals("existing-value", attemptHeaders.get(0).get("existing-header"));
        Assert.assertEquals("existing-value", attemptHeaders.get(1).get("existing-header"));
    }

    private void assertOutOfRangeWaitTimeoutSent(int waitFlyingWritersTimeoutSeconds) {
        JsonObject request = new JsonParser().parse(
                TableBatchWriteSessionImpl.generateCommitRequest(
                        messages, waitFlyingWritersTimeoutSeconds))
                .getAsJsonObject();

        Assert.assertEquals(
                waitFlyingWritersTimeoutSeconds,
                request.get("WaitFlyingWritersTimeoutSeconds").getAsInt());
    }
}
