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

package com.aliyun.odps.table.read.impl.v2;

import java.util.Arrays;
import java.util.Collections;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionRequest;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.IncrementalOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.optimizer.predicate.RawPredicate;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import org.junit.Assert;
import org.junit.Test;

/**
 * Offline unit tests for the Storage API v2 read provider: request mapping, split mode
 * guards, incremental options translation and session JSON round-trips. No live service.
 */
public class V2ReadProviderOfflineTest {

    private static EnvironmentSettings settings() {
        return EnvironmentSettings.newBuilder()
                .withCredentials(Credentials.newBuilder()
                        .withAccount(new AliyunAccount("test-id", "test-key")).build())
                .withServiceEndpoint("http://localhost:1")
                .withTunnelEndpoint("http://localhost:1")
                .build();
    }

    private static TableReadSessionBuilder baseBuilder() {
        return new TableReadSessionBuilder()
                .identifier(TableIdentifier.of("proj", "default", "tbl"))
                .withSettings(settings());
    }

    @Test
    public void testBatchRequestMappingMatchesV2WireSchema() {
        CreateTableReadSessionRequest request = V2SessionPlanner.toWireRequest(
                baseBuilder()
                        .requiredDataColumns(Arrays.asList("a", "b"))
                        .requiredPartitionColumns(Collections.singletonList("ds"))
                        .requiredPartitions(Collections.singletonList(new PartitionSpec("ds=1")))
                        .requiredBucketIds(Arrays.asList(0, 3))
                        .withSplitOptions(SplitOptions.newBuilder()
                                .SplitByByteSize(64L * 1024 * 1024)
                                .withCrossPartition(false)
                                .withBucketSort(true)
                                .withMaxFileNum(4)
                                .build())
                        .withFilterPredicate(new RawPredicate("`a` = 1"))
                        .allowFilterPredicateFallback(true)
                        .enableEstimateStats(true)
                        .enableExtendedArrowIPC(true)
                        .enableUniqueMapKey(true));

        Assert.assertEquals(Arrays.asList("a", "b"), request.getRequiredDataColumns());
        Assert.assertEquals(Collections.singletonList("ds"),
                request.getRequiredPartitionColumns());
        Assert.assertEquals(Collections.singletonList("ds=1"), request.getRequiredPartitions());
        Assert.assertEquals(Arrays.asList(0, 3), request.getRequiredBucketIds());
        Assert.assertEquals(com.aliyun.odps.storage.models.SplitMode.SIZE,
                request.getSplitOptions().getSplitMode());
        Assert.assertEquals(64L * 1024 * 1024, request.getSplitOptions().getSplitNumber());
        Assert.assertFalse(request.getSplitOptions().isCrossPartition());
        Assert.assertTrue(request.getSplitOptions().isBucketSort());
        Assert.assertEquals(4, request.getSplitMaxFileNum());
        Assert.assertEquals("`a` = 1", request.getFilterPredicate());
        Assert.assertTrue(request.isFilterPredicateFallback());
        Assert.assertTrue(request.isEnableEstimateStats());
        Assert.assertTrue(request.isEnableExtendedArrowIPC());
        Assert.assertTrue(request.isUniqueMapKey());
        Assert.assertFalse(request.isIncrementalRead());
    }

    @Test
    public void testRowOffsetSplitModeMapping() {
        CreateTableReadSessionRequest request = V2SessionPlanner.toWireRequest(
                baseBuilder().withSplitOptions(SplitOptions.newBuilder()
                        .SplitByRowOffset()
                        .build()));
        Assert.assertEquals(com.aliyun.odps.storage.models.SplitMode.ROW_OFFSET,
                request.getSplitOptions().getSplitMode());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testParallelismSplitModeFailsFast() {
        V2SessionPlanner.toWireRequest(baseBuilder().withSplitOptions(
                SplitOptions.newBuilder().SplitByParallelism(8).build()));
    }

    @Test
    public void testIncrementalVersionRangeWithTxnId() {
        TableReadSessionBuilder builder = baseBuilder().withIncrementalOptions(
                IncrementalOptions.newBuilder()
                        .startVersion(5L)
                        .endVersion(9L)
                        .startTxnId("txn-42")
                        .withIncrementalMode(IncrementalOptions.IncrementalMode.CDC)
                        .build());

        CreateTableReadSessionRequest request = V2SessionPlanner.toWireRequest(builder);
        Assert.assertTrue(request.isIncrementalRead());
        com.aliyun.odps.storage.settings.IncrementalReadOptions options =
                request.getIncrementalReadOptions();
        Assert.assertEquals(5L, options.getStartVersion());
        Assert.assertEquals(9L, options.getEndVersion());
        Assert.assertEquals("cdc", options.getMode());
        Assert.assertEquals("txn-42", options.getStartTxnId());
    }

    @Test
    public void testSessionJsonRoundTripKeepsMetadata() throws Exception {
        String details = "{"
                + "\"SessionId\":\"sess-1\","
                + "\"SessionStatus\":\"NORMAL\","
                + "\"ExpirationTime\":123456789,"
                + "\"SplitMode\":\"Size\","
                + "\"SplitsCount\":7,"
                + "\"RecordCount\":-1,"
                + "\"LatestVersion\":11,"
                + "\"LatestTxnId\":\"txn-11\","
                + "\"ExtendedArrowIPCEnabled\":true,"
                + "\"SupportedDataFormat\":[{\"Type\":\"Arrow\",\"Version\":\"V5\"}],"
                + "\"SessionStats\":{\"EstimatedSize\":10,\"EstimatedRowCount\":2},"
                + "\"DataSchema\":{\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"bigint\","
                + "\"Comment\":\"\",\"Nullable\":true}],"
                + "\"PartitionColumns\":[{\"Name\":\"ds\",\"Type\":\"string\","
                + "\"Comment\":\"\",\"Nullable\":true}]}}";
        String json = "{\"identifier\":" + quote(com.aliyun.odps.utils.JsonUtils
                .toJson(TableIdentifier.of("proj", "default", "tbl"))) + ","
                + "\"details\":" + quote(details) + "}";

        TableBatchReadSession session = new TableBatchReadSessionV2Impl(
                new TableReadSessionBuilder().withSettings(settings()).fromJson(json));

        Assert.assertEquals("sess-1", session.getId());
        Assert.assertEquals(SessionStatus.NORMAL, session.getStatus());
        Assert.assertEquals(7, session.getInputSplitAssigner().getSplitsCount());
        Assert.assertEquals(2, session.readSchema().getColumnCount());
        Assert.assertTrue(session.supportsDataFormat(
                new DataFormat(DataFormat.Type.ARROW, DataFormat.Version.V5)));
        Assert.assertEquals("txn-11", session.getLatestTxnId().orElse(""));
        // SessionStats round-trips through gson with its Java field names (estimatedSize),
        // so assert the parsed object exists plus the raw wire payload separately.
        Assert.assertNotNull(session.getEstimatedStats());
        Assert.assertTrue(details.contains("\"EstimatedRowCount\":2"));
        Assert.assertTrue(session.getSnapshot().getVersion().isPresent());
        Assert.assertEquals(11L, (long) session.getSnapshot().getVersion().orElse(-1L));
    }

    @Test
    public void testBucketSplitAssignerRebuildFromDetails() throws Exception {
        String details = "{"
                + "\"SessionId\":\"sess-b\","
                + "\"SessionStatus\":\"NORMAL\","
                + "\"SplitMode\":\"Bucket\","
                + "\"SplitsCount\":2,"
                + "\"SplitBucketId\":[0,1],"
                + "\"RecordCount\":-1,"
                + "\"DataSchema\":{\"DataColumns\":[]}}";
        String json = "{\"identifier\":" + quote(com.aliyun.odps.utils.JsonUtils
                .toJson(TableIdentifier.of("proj", "default", "tbl"))) + ","
                + "\"details\":" + quote(details) + "}";

        TableBatchReadSession session = new TableBatchReadSessionV2Impl(
                new TableReadSessionBuilder().withSettings(settings()).fromJson(json));
        Assert.assertEquals(2, session.getInputSplitAssigner().getSplitsCount());
        Assert.assertTrue(session.getInputSplitAssigner().getSplit(1)
                instanceof InputSplitWithBucket);
        Assert.assertEquals(1, ((InputSplitWithBucket)
                session.getInputSplitAssigner().getSplit(1)).getBucketId());
    }

    private static String quote(String raw) {
        return "\"" + raw.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
