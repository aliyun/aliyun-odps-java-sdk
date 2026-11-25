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

package com.aliyun.odps.table.write.impl.v2;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

/** Offline response metadata and commit-message contracts. */
public class V2WriteProviderOfflineTest {

    @Test
    public void testResponseMetadataParsingMatchesServerSchema() {
        JsonObject tree = new JsonObject();
        tree.addProperty("MaxBlockNumber", 64);
        tree.addProperty("EnhanceWriteCheck", true);
        tree.add("SupportedDataFormat", new com.google.gson.JsonParser().parse(
                "[{\"Type\":\"Arrow\",\"Version\":\"V5\"}]"));
        tree.add("RequiredOrdering", new com.google.gson.JsonParser().parse(
                "[{\"Name\":\"id\",\"SortDirection\":\"ASC\",\"NullOrdering\":\"NULLS_LAST\"}]"));
        tree.add("RequiredDistribution", new com.google.gson.JsonParser().parse(
                "{\"Type\":\"HASH\",\"ClusterKeys\":[\"id\"],\"BucketsNumber\":16}"));
        tree.add("DataSchema", new com.google.gson.JsonParser().parse(
                "{\"DataColumns\":[{\"Name\":\"id\",\"Type\":\"bigint\",\"Comment\":\"\","
                        + "\"Nullable\":true}],"
                        + "\"PartitionColumns\":[{\"Name\":\"ds\",\"Type\":\"string\","
                        + "\"Comment\":\"\",\"Nullable\":true}]}"));

        Assert.assertEquals(64, tree.get("MaxBlockNumber").getAsLong());
        com.aliyun.odps.table.order.SortOrder[] orders =
                V2WriteResponses.parseRequiredOrdering(tree);
        Assert.assertEquals(1, orders.length);
        Assert.assertEquals("id", orders[0].name());
        Assert.assertEquals(com.aliyun.odps.table.order.SortDirection.ASC,
                orders[0].direction());
        Assert.assertEquals(com.aliyun.odps.table.order.NullOrdering.NULLS_LAST,
                orders[0].nullOrdering());
        Assert.assertEquals(com.aliyun.odps.table.distribution.Distribution.Type.HASH,
                V2WriteResponses.parseRequiredDistribution(tree).getType());
        Assert.assertEquals(2,
                V2WriteResponses.parseDataSchema(tree.getAsJsonObject("DataSchema"))
                        .getColumnCount()); // data + partition columns
        Assert.assertEquals(1, V2WriteResponses.parseSupportedDataFormats(tree).size());
    }

    @Test
    public void testCommitMessageChecks() throws Exception {
        String message = "{\"BlockNumber\":3,\"AttemptNumber\":1,\"RecordNum\":7}";
        // Matching block/attempt passes.
        V2CommitMessages.check(message, 3, 1);
        Assert.assertEquals(7, V2CommitMessages.recordCount(message, -1));
        try {
            V2CommitMessages.check(message, 4, 1);
            Assert.fail("block mismatch must throw");
        } catch (java.io.IOException expected) {
            Assert.assertTrue(expected.getMessage().contains("Unexpected block number"));
        }
        try {
            V2CommitMessages.check(message, 3, 2);
            Assert.fail("attempt mismatch must throw");
        } catch (java.io.IOException expected) {
            Assert.assertTrue(expected.getMessage().contains("Unexpected attempt number"));
        }
        // Missing stats fall back to the caller-provided count.
        Assert.assertEquals(5L,
                V2CommitMessages.recordCount("{}", 5L));
    }
}
