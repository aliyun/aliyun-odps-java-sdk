package com.aliyun.odps.storage.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.aliyun.odps.Column;
import com.aliyun.odps.storage.internal.StorageStub;
import com.aliyun.odps.storage.internal.models.CreateTableReadSessionResponse;
import com.aliyun.odps.storage.internal.models.ReadSchema;
import com.aliyun.odps.storage.models.SplitMode;
import com.aliyun.odps.storage.settings.SplitOptions;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.RowRange;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.aliyun.odps.type.TypeInfoFactory;

public class TableReadSessionTest {

    @Mock
    private StorageStub mockStorageStub;

    private BufferAllocator mockAllocator = new RootAllocator(0);

    @Mock
    private CreateTableReadSessionResponse mockResponse;

    @Mock
    private ReadSchema mockTableSchema;

    private final Column mockColumn1 = new Column("col1", TypeInfoFactory.STRING);

    private final Column mockColumn2 = new Column("col2", TypeInfoFactory.BIGINT);

    private TableIdentifier tableId;

    @Mock
    private TableReadSessionBuilder builder;

    private TableReadSession tableReadSession;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        tableId = TableIdentifier.of("test_project", "test_table");

        // Set up mock behavior
        when(mockResponse.getSessionId()).thenReturn("test_session_id");
        when(mockResponse.getDataSchema()).thenReturn(mockTableSchema);

        when(mockResponse.getSplitMode()).thenReturn(SplitMode.ROW_OFFSET);
        when(mockResponse.getSplitsCount()).thenReturn(3);

        when(mockTableSchema.getAllColumns()).thenReturn(Arrays.asList(mockColumn1, mockColumn2));

        SplitOptions splitOptions = SplitOptions.newBuilder()
          .withSplitRowCount(3)
          .build();
        when(builder.getSplitOptions()).thenReturn(splitOptions);
    }

    @After
    public void tearDown() {
        if (tableReadSession != null) {
            tableReadSession.close();
        }
    }

    @Test
    public void testConstructor() {
        tableReadSession =
          new TableReadSession(mockStorageStub, tableId, mockAllocator, mockResponse, builder);

        assertNotNull(tableReadSession);
        assertEquals("test_session_id", tableReadSession.getId());
        assertEquals(mockTableSchema, tableReadSession.getTableSchema());
    }

    @Test
    public void testGetSizeBasedSplits() {
        when(mockResponse.getSplitMode()).thenReturn(SplitMode.SIZE);
        when(mockResponse.getSplitsCount()).thenReturn(3);

        tableReadSession =
          new TableReadSession(mockStorageStub, tableId, mockAllocator, mockResponse, builder);
        List<InputSplit> splits = tableReadSession.getSplits();

        assertNotNull(splits);
        assertEquals(3, splits.size());

        for (int i = 0; i < splits.size(); i++) {
            InputSplit split = splits.get(i);
            assertTrue(split instanceof IndexedInputSplit);
            IndexedInputSplit indexedSplit = (IndexedInputSplit) split;
            assertEquals("test_session_id", indexedSplit.getSessionId());
            assertEquals(i, indexedSplit.getSplitIndex());
        }
    }

    @Test
    public void testGetRowOffsetBasedSplits() {
        when(mockResponse.getSplitMode()).thenReturn(SplitMode.ROW_OFFSET);
        when(mockResponse.getRecordCount()).thenReturn(30L);

        tableReadSession =
          new TableReadSession(mockStorageStub, tableId, mockAllocator, mockResponse, builder);
        List<InputSplit> splits = tableReadSession.getSplits();

        assertNotNull(splits);
        assertEquals(10, splits.size()); // 10 records with split size 1

        for (int i = 0; i < splits.size(); i++) {
            InputSplit split = splits.get(i);
            assertTrue(split instanceof RowRangeInputSplit);
            RowRangeInputSplit rangeSplit = (RowRangeInputSplit) split;
            assertEquals("test_session_id", rangeSplit.getSessionId());
            RowRange rowRange = rangeSplit.getRowRange();
            System.out.println(rowRange);
        }
    }

    @Test
    public void testGetRowOffsetBasedSplitsWithLargerSplitSize() {
        when(mockResponse.getSplitMode()).thenReturn(SplitMode.ROW_OFFSET);
        when(mockResponse.getRecordCount()).thenReturn(10L);

        tableReadSession =
          new TableReadSession(mockStorageStub, tableId, mockAllocator, mockResponse, builder);
        List<InputSplit> splits = tableReadSession.getSplits();

        assertNotNull(splits);
        assertEquals(4, splits.size()); // 10 records with split size 3: 3+3+3+1

        // Check first split
        RowRangeInputSplit split1 = (RowRangeInputSplit) splits.get(0);
        assertEquals("test_session_id", split1.getSessionId());
        assertEquals(0L, split1.getRowRange().getStartIndex());
        assertEquals(3L, split1.getRowRange().getNumRecord());

        // Check last split
        RowRangeInputSplit split4 = (RowRangeInputSplit) splits.get(3);
        assertEquals("test_session_id", split4.getSessionId());
        assertEquals(9L, split4.getRowRange().getStartIndex());
        assertEquals(1L, split4.getRowRange().getNumRecord()); // remaining record
    }

    @Test
    public void testGetArrowSchema() {
        tableReadSession =
          new TableReadSession(mockStorageStub, tableId, mockAllocator, mockResponse, builder);
        Schema arrowSchema = tableReadSession.getArrowSchema();

        assertNotNull(arrowSchema);
        assertEquals(2, arrowSchema.getFields().size());
        assertEquals("col1", arrowSchema.getFields().get(0).getName());
        assertEquals("col2", arrowSchema.getFields().get(1).getName());
    }

    @Test
    public void testClose() {
        tableReadSession =
          new TableReadSession(mockStorageStub, tableId, mockAllocator, mockResponse, builder);
        tableReadSession.close();
        // Should not throw any exception
    }

    @Test
    public void testEmptySplitsCase() {
        when(mockResponse.getSplitMode()).thenReturn(SplitMode.ROW_OFFSET);
        when(mockResponse.getRecordCount()).thenReturn(0L);

        tableReadSession =
          new TableReadSession(mockStorageStub, tableId, mockAllocator, mockResponse, builder);
        List<InputSplit> splits = tableReadSession.getSplits();

        assertNotNull(splits);
        assertEquals(0, splits.size());
    }
}