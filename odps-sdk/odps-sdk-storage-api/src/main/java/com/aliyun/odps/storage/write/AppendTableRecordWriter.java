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

package com.aliyun.odps.storage.write;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.arrow.vector.VectorSchemaRoot;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Blob;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.storage.ClientException;
import com.aliyun.odps.storage.internal.models.WriteSchema;
import com.aliyun.odps.storage.internal.utils.IOUtils;
import com.aliyun.odps.table.arrow.constructor.ArrowBatchConstructor;
import com.aliyun.odps.table.record.constructor.RecordToArrowConverter;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class AppendTableRecordWriter implements RecordWriter {

  private final TableArrowWriter arrowWriter;

  private final ArrowBatchConstructor<ArrayRecord> recordToArrowConverter;

  private long rowCount;

  private final long rowCountPerBatch;

  private final boolean batchBlobUploadEnabled;

  // Blob column indices for per-row mimeType collection (only used in batch mode)
  private final List<Integer> blobColumnIndices;


  public AppendTableRecordWriter(TableArrowWriter arrowWriter, long recordCountPerBatch) {
    this.arrowWriter = arrowWriter;
    this.recordToArrowConverter =
      RecordToArrowConverter.createRecordArrowBatchConstructor(
        arrowWriter.getWriteSchema().getColumns(), arrowWriter.getAllocator());
    this.rowCountPerBatch = recordCountPerBatch;
    this.batchBlobUploadEnabled = arrowWriter instanceof TableArrowBatchBlobWriter;
    this.blobColumnIndices = batchBlobUploadEnabled
        ? ((TableArrowBatchBlobWriter) arrowWriter).getBlobColumnIndices()
        : null;
  }

  /**
   * Record can be reused, and recommend reuse it.
   */
  @Override
  public Record newRecord(boolean caseSensitive) {
    if (batchBlobUploadEnabled) {
      return new BatchBlobRecord(
          arrowWriter.getWriteSchema(), false, null, caseSensitive);
    }
    return new BlobUploadableRecord((is, cid) -> uploadBlob(cid, is),
                                    arrowWriter.getWriteSchema(), false, null,
                           caseSensitive);
  }

  public void write(Record r) {
    if (!(r instanceof ArrayRecord)) {
      throw new ClientException("Record must be ArrayRecord");
    }

    if (batchBlobUploadEnabled) {
      collectBlobMimeTypes((ArrayRecord) r);
    }

    recordToArrowConverter.write((ArrayRecord) r);
    rowCount++;

    if (rowCount >= rowCountPerBatch) {
      flushRecords();
    }
  }

  /**
   * Extract per-row mimeType from Blob columns and accumulate in the batch writer.
   */
  private void collectBlobMimeTypes(ArrayRecord record) {
    TableArrowBatchBlobWriter batchWriter = (TableArrowBatchBlobWriter) arrowWriter;
    for (int colIdx : blobColumnIndices) {
      Object val = record.get(colIdx);
      String mime = (val instanceof Blob) ? ((Blob) val).getMimeType() : null;
      batchWriter.accumulateRowMimeType(colIdx, mime);
    }
  }

  public long bytesWritten() {
    return arrowWriter.bytesWritten();
  }

  public void flush() {
    flushRecords();
    arrowWriter.flush();
  }

  private void flushRecords() {
    if (rowCount > 0) {
      recordToArrowConverter.finish();
      VectorSchemaRoot vectorSchemaRoot = recordToArrowConverter.getVectorSchemaRoot();
      arrowWriter.writeBatch(vectorSchemaRoot);
      recordToArrowConverter.reset();
      rowCount = 0;
    }
  }

  public Blob uploadBlob(long columnId, InputStream data) {
    return arrowWriter.uploadBlob(columnId, data);
  }

  public void close() {
    flush();
    arrowWriter.close();
    recordToArrowConverter.getVectorSchemaRoot().close();
  }

  /**
   * Returns the request ID of the last write (flush) for this stream. For client-side logging.
   */
  public String getLastRequestId() {
    return arrowWriter.getLastRequestId();
  }

  private static class BlobUploadableRecord extends ArrayRecord {

    private final WriteSchema writeSchema;

    private final BiFunction<InputStream, Long, Blob> uploader;

    public BlobUploadableRecord(BiFunction<InputStream, Long, Blob> uploader,
                                WriteSchema schema,
                                boolean strictTypeValidation,
                                Long fieldMaxSize,
                                boolean caseSensitive) {
      super(schema.getColumns().toArray(new Column[0]), strictTypeValidation, fieldMaxSize,
            caseSensitive);
      this.writeSchema = schema;
      this.uploader = uploader;
    }

    @Override
    public void set(int idx, Object value) {
      if (value instanceof Blob) {
        Blob blob = (Blob) value;
        Column column = writeSchema.getColumns().get(idx);
        if (blob.isRawStream()) {
          value = blob.withUploader(this.uploader, column.getColumnId());
        }
      }
      super.set(idx, value);
    }
  }

  /**
   * Record implementation for batch blob upload mode.
   * Reads raw InputStream to byte[] so that raw bytes are written to VarBinaryVector,
   * and later batch-uploaded by {@link TableArrowBatchBlobWriter#writeBatch}.
   */
  private static class BatchBlobRecord extends ArrayRecord {

    private final WriteSchema writeSchema;

    public BatchBlobRecord(WriteSchema schema,
                           boolean strictTypeValidation,
                           Long fieldMaxSize,
                           boolean caseSensitive) {
      super(schema.getColumns().toArray(new Column[0]), strictTypeValidation, fieldMaxSize,
            caseSensitive);
      this.writeSchema = schema;
    }

    @Override
    public void set(int idx, Object value) {
      if (value instanceof Blob) {
        Blob blob = (Blob) value;
        if (blob.isRawStream()) {
          try {
            value = Blob.fromBytes(
                IOUtils.readAllBytes(blob.getRawStream()), blob.getMimeType());
          } catch (IOException e) {
            throw new ClientException("Failed to read blob input stream.", e);
          }
        }
      }
      super.set(idx, value);
    }
  }
}
