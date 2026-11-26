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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionRequest;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.models.SplitMode;
import com.aliyun.odps.storage.settings.SplitOptions;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.RowRange;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * {@link TableReadSession} 的构造、切分与 Arrow schema 推导，全部离线（不发请求、不碰凭据）。
 *
 * <p>response 用 {@link StorageStub#createWireGson()} 从线上 JSON 反序列化成真实对象，不再用
 * Mockito 桩：构造函数会把整个 response 反射式序列化进 descriptorMetadata
 * （{@code TableReadSession.java:108}），而桩对象的 mockito 内部字段带着 {@code java.lang.Class}，
 * gson 直接抛 {@code UnsupportedOperationException: Attempted to serialize java.lang.Class}。
 * 这七条用例在 JUnit 4 时代一条都没执行过（the original regression），所以这个形状一直没人看见。
 */
class TableReadSessionTest {

    private static final String SESSION_ID = "test_session_id";

    private static final String DATA_SCHEMA_JSON
            = "\"DataSchema\":{\"DataColumns\":["
            + "{\"Name\":\"col1\",\"Type\":\"string\",\"ColumnId\":1},"
            + "{\"Name\":\"col2\",\"Type\":\"bigint\",\"ColumnId\":2}],"
            + "\"PartitionColumns\":[],\"SystemColumns\":[],\"BlobColumns\":[]}";

    @Mock
    private StorageStub mockStorageStub;

    @Mock
    private TableReadSessionBuilder mockBuilder;

    @Mock
    private CreateTableReadSessionRequest mockRequest;

    private final BufferAllocator mockAllocator = new RootAllocator(0);

    private TableIdentifier tableId;

    private TableReadSession tableReadSession;

    private AutoCloseable mockitoSession;

    @BeforeEach
    void setUp() {
        mockitoSession = MockitoAnnotations.openMocks(this);

        tableId = TableIdentifier.of("test_project", "test_table");

        // Set up mock behavior
        when(mockBuilder.getSplitOptions())
                .thenReturn(SplitOptions.newBuilder().withSplitRowCount(3).build());
        when(mockBuilder.getRequest()).thenReturn(mockRequest);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (tableReadSession != null) {
            tableReadSession.close();
        }
        mockitoSession.close();
    }

    /** 线上 JSON 形状的响应：SplitMode 走 wire 名（"Size" / "RowOffset"）。 */
    private static CreateTableReadSessionResponse response(String splitMode, int splitsCount,
            long recordCount) {
        return StorageStub.createWireGson().fromJson(
                "{\"SessionId\":\"" + SESSION_ID + "\","
                        + "\"SessionStatus\":\"NORMAL\","
                        + "\"SplitMode\":\"" + splitMode + "\","
                        + "\"SplitsCount\":" + splitsCount + ","
                        + "\"RecordCount\":" + recordCount + ","
                        + DATA_SCHEMA_JSON + "}",
                CreateTableReadSessionResponse.class);
    }

    private TableReadSession newSession(CreateTableReadSessionResponse response) {
        return new TableReadSession(mockStorageStub, tableId, mockAllocator, response, mockBuilder);
    }

    @Test
    void testConstructor() {
        CreateTableReadSessionResponse response = response(SplitMode.SIZE.toString(), 3, -1L);
        tableReadSession = newSession(response);

        assertNotNull(tableReadSession);
        assertEquals(SESSION_ID, tableReadSession.getId());
        assertSame(response.getDataSchema(), tableReadSession.getTableSchema());
    }

    @Test
    void testGetSizeBasedSplits() {
        tableReadSession = newSession(response(SplitMode.SIZE.toString(), 3, -1L));
        List<InputSplit> splits = tableReadSession.getSplits();

        assertNotNull(splits);
        assertEquals(3, splits.size());

        for (int i = 0; i < splits.size(); i++) {
            InputSplit split = splits.get(i);
            assertTrue(split instanceof IndexedInputSplit);
            IndexedInputSplit indexedSplit = (IndexedInputSplit) split;
            assertEquals(SESSION_ID, indexedSplit.getSessionId());
            assertEquals(i, indexedSplit.getSplitIndex());
        }
    }

    @Test
    void testGetRowOffsetBasedSplits() {
        tableReadSession = newSession(response(SplitMode.ROW_OFFSET.toString(), 0, 30L));
        List<InputSplit> splits = tableReadSession.getSplits();

        assertNotNull(splits);
        assertEquals(10, splits.size()); // 30 records with 3 rows per split

        for (InputSplit split : splits) {
            assertTrue(split instanceof RowRangeInputSplit);
            RowRangeInputSplit rangeSplit = (RowRangeInputSplit) split;
            assertEquals(SESSION_ID, rangeSplit.getSessionId());
            RowRange rowRange = rangeSplit.getRowRange();
            assertNotNull(rowRange);
        }
    }

    @Test
    void testGetRowOffsetBasedSplitsWithLargerSplitSize() {
        tableReadSession = newSession(response(SplitMode.ROW_OFFSET.toString(), 0, 10L));
        List<InputSplit> splits = tableReadSession.getSplits();

        assertNotNull(splits);
        assertEquals(4, splits.size()); // 10 records with split size 3: 3+3+3+1

        // Check first split
        RowRangeInputSplit split1 = (RowRangeInputSplit) splits.get(0);
        assertEquals(SESSION_ID, split1.getSessionId());
        assertEquals(0L, split1.getRowRange().getStartIndex());
        assertEquals(3L, split1.getRowRange().getNumRecord());

        // Check last split
        RowRangeInputSplit split4 = (RowRangeInputSplit) splits.get(3);
        assertEquals(SESSION_ID, split4.getSessionId());
        assertEquals(9L, split4.getRowRange().getStartIndex());
        assertEquals(1L, split4.getRowRange().getNumRecord()); // remaining record
    }

    @Test
    void testGetArrowSchema() {
        tableReadSession = newSession(response(SplitMode.SIZE.toString(), 1, -1L));
        Schema arrowSchema = tableReadSession.getArrowSchema();

        assertNotNull(arrowSchema);
        assertEquals(2, arrowSchema.getFields().size());
        assertEquals("col1", arrowSchema.getFields().get(0).getName());
        assertEquals("col2", arrowSchema.getFields().get(1).getName());
    }

    @Test
    void testClose() {
        tableReadSession = newSession(response(SplitMode.SIZE.toString(), 1, -1L));
        tableReadSession.close();
        // Should not throw any exception
    }

    @Test
    void testEmptySplitsCase() {
        tableReadSession = newSession(response(SplitMode.ROW_OFFSET.toString(), 0, 0L));
        List<InputSplit> splits = tableReadSession.getSplits();

        assertNotNull(splits);
        assertEquals(0, splits.size());
    }
}
