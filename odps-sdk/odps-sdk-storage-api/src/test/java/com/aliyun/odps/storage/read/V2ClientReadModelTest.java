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

package com.aliyun.odps.storage.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.internal.models.ReadSchema;
import com.aliyun.odps.storage.models.SplitMode;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import com.aliyun.odps.table.read.split.InputSplitWithIndex;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * Offline coverage for the Storage API v2 client changes made for the table-api v2
 * provider: BUCKET split materialization and tolerant schema deserialization.
 */
public class V2ClientReadModelTest {

    private static CreateTableReadSessionResponse bucketResponse() {
        CreateTableReadSessionResponse response = new CreateTableReadSessionResponse();
        response.setSessionId("sess-bucket");
        response.setSessionStatus("NORMAL");
        response.setSplitMode(SplitMode.BUCKET);
        response.setSplitsCount(3);
        response.setSplitBucketId(Arrays.asList(2, 0, 1));
        ReadSchema schema = new ReadSchema();
        schema.setColumns(Collections.singletonList(new Column("a", TypeInfoFactory.BIGINT)));
        schema.setPartitionColumns(Collections.emptyList());
        schema.setSystemColumns(Collections.emptyList());
        schema.setBlobColumns(Collections.emptyList());
        response.setDataSchema(schema);
        return response;
    }

    @Test
    public void testBucketSplitModeMaterializesBucketSplits() {
        TableIdentifier tableId = TableIdentifier.of("proj", "default", "tbl");
        TableReadSession session = new TableReadSession(
                null, tableId, null, bucketResponse(),
                new TableReadSessionBuilder(null, null, tableId));

        assertEquals(3, session.getSplits().size());
        InputSplit first = session.getSplits().get(0);
        assertInstanceOf(InputSplitWithBucket.class, first);
        assertInstanceOf(InputSplitWithIndex.class, first);
        assertEquals(2, ((InputSplitWithBucket) first).getBucketId());
        assertEquals(0, ((InputSplitWithIndex) first).getSplitIndex());
        assertEquals(1, ((InputSplitWithBucket) session.getSplits().get(2)).getBucketId());
    }

    @Test
    public void testReadSchemaDeserializationToleratesMissingOptionalFields() {
        String wire = "{\"DataColumns\":[{\"Name\":\"a\",\"Type\":\"bigint\"}],"
                + "\"PartitionColumns\":[]}";
        ReadSchema schema = STUB_GSON.fromJson(wire, ReadSchema.class);
        assertEquals(1, schema.getColumns().size());
        assertEquals("a", schema.getColumns().get(0).getName());
        // Comment defaults to empty, Nullable defaults to true, ColumnId stays unset.
        assertEquals("", schema.getColumns().get(0).getComment());
        assertTrue(schema.getColumns().get(0).isNullable());
    }

    private static final com.google.gson.Gson STUB_GSON =
            new com.google.gson.GsonBuilder()
                    .registerTypeAdapter(ReadSchema.class,
                            new com.aliyun.odps.storage.internal.serializer
                                    .ReadSchemaDeserializer())
                    .create();
}
