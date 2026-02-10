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

package com.aliyun.odps.table.arrow.compression;

import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

public class OdpsCompressionFactory implements CompressionCodec.Factory {

    public static final OdpsCompressionFactory INSTANCE = new OdpsCompressionFactory();

    public CompressionCodec createCodec(
      com.aliyun.odps.table.configuration.CompressionCodec codecType) {
        switch (codecType) {
            case LZ4_FRAME:
                return new OdpsLz4CompressionCodec();
            case ZSTD:
                return new OdpsZstdCompressionCodec();
            default:
                throw new IllegalArgumentException("Compression type not supported: " + codecType);
        }
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
        switch (codecType) {
            case LZ4_FRAME:
                return new OdpsLz4CompressionCodec();
            case ZSTD:
                return new OdpsZstdCompressionCodec();
            default:
                throw new IllegalArgumentException("Compression type not supported: " + codecType);
        }
    }

    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
        switch (codecType) {
            case LZ4_FRAME:
                return new OdpsLz4CompressionCodec();
            case ZSTD:
                return new OdpsZstdCompressionCodec(compressionLevel);
            default:
                throw new IllegalArgumentException("Compression type not supported: " + codecType);
        }
    }
}