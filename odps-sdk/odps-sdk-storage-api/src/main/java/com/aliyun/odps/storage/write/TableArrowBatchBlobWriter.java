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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.internal.models.BlobWriteItem;
import com.aliyun.odps.storage.internal.models.BlobWriteResponse;
import com.aliyun.odps.storage.internal.models.CreateWriteStreamResponse;
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
  private final BlobWriteItem.ChecksumType blobChecksumType;

  TableArrowBatchBlobWriter(TableWriterBuilder builder,
                            CreateWriteStreamResponse response) {
    super(builder, response);
    this.blobColumnIndices = new ArrayList<>();
    this.blobColumnIds = new ArrayList<>();
    this.blobChecksumType = builder.getBlobChecksumType();

    List<Column> columns = tableSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      if (column.getTypeInfo().getOdpsType() == OdpsType.BLOB) {
        blobColumnIndices.add(i);
        blobColumnIds.add(column.getColumnId());
      }
    }
  }

  @Override
  public void writeBatch(VectorSchemaRoot root) {
    if (root == null || root.getRowCount() == 0) {
      return;
    }

    if (!blobColumnIndices.isEmpty()) {
      processBlobColumns(root);
    }

    super.writeBatch(root);
  }

  private void processBlobColumns(VectorSchemaRoot root) {
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
          .build();
        itemsToWrite.add(item);
      }
      if (itemsToWrite.isEmpty()) {
        return;
      }
      BlobWriteResponse
        blobWriteResponse =
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
        ((VarBinaryVector) blobVector).setSafe(originalRow, Base64.getDecoder().decode(reference));
      }
    }
  }
}