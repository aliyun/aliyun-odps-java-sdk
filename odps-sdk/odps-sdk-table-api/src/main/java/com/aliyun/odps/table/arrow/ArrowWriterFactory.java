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

package com.aliyun.odps.table.arrow;

import com.aliyun.odps.table.arrow.writers.ArrowBatchWriter;
import com.aliyun.odps.table.configuration.WriterOptions;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A factory used to create {@link ArrowWriter} instances.
 */
public class ArrowWriterFactory {

    public static ArrowWriter getRecordBatchWriter(OutputStream os,
                                                   WriterOptions writerOptions) throws IOException {
        switch (writerOptions.getCompressionCodec()) {
            case NO_COMPRESSION:
                return new ArrowBatchWriter(os);
            case ZSTD:
                return new ArrowBatchWriter(os, CompressionUtil.CodecType.ZSTD);
            case LZ4_FRAME:
                return new ArrowBatchWriter(os, CompressionUtil.CodecType.LZ4_FRAME);
            default:
                throw new IllegalArgumentException("Unsupported compression codec: " +
                        writerOptions.getCompressionCodec());
        }
    }
}
