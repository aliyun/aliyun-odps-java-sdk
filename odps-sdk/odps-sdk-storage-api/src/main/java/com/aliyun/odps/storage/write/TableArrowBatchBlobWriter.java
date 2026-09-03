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
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.storage.write;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.internal.models.BlobWriteItem;
import com.aliyun.odps.storage.internal.models.BlobWriteResponse;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
import com.aliyun.odps.storage.internal.models.WriteSchema;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TableArrowBatchBlobWriter extends TableArrowWriter {

  private static final Logger LOG = LoggerFactory.getLogger(TableArrowBatchBlobWriter.class);
  private static final Gson GSON = new GsonBuilder().create();

  private final List<Integer> blobColumnIndices;
  private final List<Long> blobColumnIds;
  private final List<NestedBlobEntry> nestedBlobEntries;
  private final BlobWriteItem.ChecksumType blobChecksumType;
  private final String blobMimeType;
  private final String blobCustomFileName;

  // Per-batch accumulator for per-row mimeType: columnIndex → list of mimeType (indexed by row)
  private final Map<Integer, List<String>> rowMimeTypeAccumulator = new HashMap<>();

  // Per-batch accumulator for per-row customFileName: columnIndex → list of customFileName (indexed by row)
  private final Map<Integer, List<String>> rowCustomFileNameAccumulator = new HashMap<>();

  TableArrowBatchBlobWriter(TableWriterBuilder builder,
                            CreateWriteStreamResponse response) {
    super(builder, response);
    this.blobColumnIndices = new ArrayList<>();
    this.blobColumnIds = new ArrayList<>();
    this.nestedBlobEntries = new ArrayList<>();
    this.blobChecksumType = builder.getBlobChecksumType();
    this.blobMimeType = builder.getBlobMimeType();
    this.blobCustomFileName = builder.getBlobCustomFileName();

    Map<String, Long> allBlobIds = tableSchema.findAllBlobColumnIds();
    List<Column> columns = tableSchema.getColumns();
    for (Map.Entry<String, Long> entry : allBlobIds.entrySet()) {
      String path = entry.getKey();
      long columnId = entry.getValue();
      if (path.contains(".")) {
        nestedBlobEntries.add(new NestedBlobEntry(path, columnId));
      } else {
        for (int i = 0; i < columns.size(); i++) {
          if (columns.get(i).getName().equals(path)) {
            blobColumnIndices.add(i);
            blobColumnIds.add(columnId);
            break;
          }
        }
      }
    }
  }

  private static class NestedBlobEntry {
    final String path;
    final long columnId;

    NestedBlobEntry(String path, long columnId) {
      this.path = path;
      this.columnId = columnId;
    }
  }

  /**
   * Accumulate per-row mimeType for a specific blob column.
   * Called by {@link AppendTableRecordWriter} for each row before writeBatch.
   *
   * @param columnIndex the schema column index of the blob column
   * @param mimeType the mimeType for this row (null if not set)
   */
  void accumulateRowMimeType(int columnIndex, String mimeType) {
    rowMimeTypeAccumulator.computeIfAbsent(columnIndex, k -> new ArrayList<>()).add(mimeType);
  }

  /**
   * Accumulate per-row customFileName for a specific blob column.
   * Called by {@link AppendTableRecordWriter} for each row before writeBatch.
   *
   * @param columnIndex the schema column index of the blob column
   * @param customFileName the customFileName for this row (null if not set)
   */
  void accumulateRowCustomFileName(int columnIndex, String customFileName) {
    rowCustomFileNameAccumulator.computeIfAbsent(columnIndex, k -> new ArrayList<>())
        .add(customFileName);
  }

  List<Integer> getBlobColumnIndices() {
    return blobColumnIndices;
  }

  @Override
  public void writeBatch(VectorSchemaRoot root) {
    if (root == null || root.getRowCount() == 0) {
      return;
    }

    if (!blobColumnIndices.isEmpty()) {
      processTopLevelBlobColumns(root);
    }

    if (!nestedBlobEntries.isEmpty()) {
      processNestedBlobColumns(root);
    }

    rowMimeTypeAccumulator.clear();
    rowCustomFileNameAccumulator.clear();
    super.writeBatch(root);
  }

  private void processTopLevelBlobColumns(VectorSchemaRoot root) {
    int rowCount = root.getRowCount();

    for (int blobIdx = 0; blobIdx < blobColumnIndices.size(); blobIdx++) {
      int columnIndex = blobColumnIndices.get(blobIdx);
      long columnId = blobColumnIds.get(blobIdx);
      FieldVector blobVector = root.getVector(columnIndex);

      if (!(blobVector instanceof VarBinaryVector)) {
        throw new ClientException("Blob vector must be VarBinaryVector.");
      }

      List<Integer> originalRowIndices = new ArrayList<>();
      List<BlobWriteItem> itemsToWrite = new ArrayList<>();
      for (int row = 0; row < rowCount; row++) {
        if (blobVector.isNull(row)) {
          continue;
        }
        byte[] blobData = ((VarBinaryVector) blobVector).get(row);
        originalRowIndices.add(row);

        BlobWriteItem item = BlobWriteItem.builder()
          .data(blobData)
          .withChecksum(blobChecksumType)
          .columnId(columnId)
          .mimeType(resolveRowMimeType(columnIndex, row))
          .customFileName(resolveRowCustomFileName(columnIndex, row))
          .distributionKey(generateDistributionKeyString(root, row, primaryKeyColumnIndices))
          .build();
        itemsToWrite.add(item);
      }
      if (itemsToWrite.isEmpty()) {
        continue;
      }
      replaceBlobReferences(
          (VarBinaryVector) blobVector, originalRowIndices, itemsToWrite);
    }
  }

  private void processNestedBlobColumns(VectorSchemaRoot root) {
    for (NestedBlobEntry entry : nestedBlobEntries) {
      VarBinaryVector blobVector = resolveBlobVector(root, entry.path);
      if (blobVector == null) {
        continue;
      }
      int valueCount = blobVector.getValueCount();

      List<Integer> originalRowIndices = new ArrayList<>();
      List<BlobWriteItem> itemsToWrite = new ArrayList<>();
      for (int row = 0; row < valueCount; row++) {
        if (blobVector.isNull(row)) {
          continue;
        }
        byte[] blobData = blobVector.get(row);
        originalRowIndices.add(row);

        BlobWriteItem item = BlobWriteItem.builder()
          .data(blobData)
          .withChecksum(blobChecksumType)
          .columnId(entry.columnId)
          .mimeType(blobMimeType)
          .customFileName(blobCustomFileName)
          .build();
        itemsToWrite.add(item);
      }
      if (itemsToWrite.isEmpty()) {
        continue;
      }
      replaceBlobReferences(blobVector, originalRowIndices, itemsToWrite);
    }
  }

  private void replaceBlobReferences(VarBinaryVector blobVector,
                                     List<Integer> originalRowIndices,
                                     List<BlobWriteItem> itemsToWrite) {
    BlobWriteResponse blobWriteResponse =
        storageStub.tableBatchWriteBlob(tableId, sessionId, streamId, streamVersion, itemsToWrite);

    List<String> receivedReferences = blobWriteResponse.getBlobReferences();

    if (receivedReferences.size() != itemsToWrite.size()) {
      throw new ClientException(
        String.format("Mismatch between sent items (%d) and received references (%d).",
                      itemsToWrite.size(), receivedReferences.size()));
    }
    for (int j = 0; j < receivedReferences.size(); j++) {
      String reference = receivedReferences.get(j);
      int originalRow = originalRowIndices.get(j);
      blobVector.setSafe(originalRow, Base64.getDecoder().decode(reference));
    }
  }

  private VarBinaryVector resolveBlobVector(VectorSchemaRoot root, String path) {
    String[] parts = path.split("\\.");
    FieldVector vector = root.getVector(parts[0]);
    if (vector == null) {
      return null;
    }
    for (int i = 1; i < parts.length; i++) {
      if (vector instanceof ListVector) {
        vector = (FieldVector) ((ListVector) vector).getDataVector();
      } else if (vector instanceof StructVector) {
        vector = ((StructVector) vector).getChild(parts[i]);
      } else {
        return null;
      }
      if (vector == null) {
        return null;
      }
    }
    if (!(vector instanceof VarBinaryVector)) {
      throw new ClientException(
        "Resolved blob vector must be VarBinaryVector, but got: " + vector.getClass().getName());
    }
    return (VarBinaryVector) vector;
  }

  /**
   * Resolve mimeType for a specific row and column.
   * Per-row mimeType (from accumulator) takes precedence over builder-level default.
   */
  private String resolveRowMimeType(int columnIndex, int row) {
    List<String> rowMimes = rowMimeTypeAccumulator.get(columnIndex);
    if (rowMimes != null && row < rowMimes.size() && rowMimes.get(row) != null) {
      return rowMimes.get(row);
    }
    return blobMimeType;
  }

  /**
   * Resolve customFileName for a specific row and column.
   * Per-row customFileName (from accumulator) takes precedence over builder-level default.
   */
  private String resolveRowCustomFileName(int columnIndex, int row) {
    List<String> rowNames = rowCustomFileNameAccumulator.get(columnIndex);
    if (rowNames != null && row < rowNames.size() && rowNames.get(row) != null) {
      return rowNames.get(row);
    }
    return blobCustomFileName;
  }
}