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
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Native-preferred LZ4 decoder selection shared by Arrow read paths. */
final class Lz4ReadCompressionFactory implements CompressionCodec.Factory {
  static final String FORCE_JAVA_PROPERTY = "odps.table.lz4.forceJava";
  private static final String LEGACY_FORCE_JAVA_PROPERTY = "odps.table.v2.lz4.forceJava";
  private static final Logger LOG = LoggerFactory.getLogger(Lz4ReadCompressionFactory.class);

  Lz4ReadCompressionFactory() {}

  @Override
  public CompressionCodec createCodec(CodecType type) {
    return type == CodecType.LZ4_FRAME ? PreferredLz4.CODEC
        : OdpsCompressionFactory.INSTANCE.createCodec(type);
  }

  @Override
  public CompressionCodec createCodec(CodecType type, int level) {
    return type == CodecType.LZ4_FRAME ? PreferredLz4.CODEC
        : OdpsCompressionFactory.INSTANCE.createCodec(type, level);
  }

  // Initialize only when a stream actually contains LZ4. The class initializer serializes
  // concurrent first reads and remembers both successful native loading and Java fallback.
  private static final class PreferredLz4 {
    private static final CompressionCodec CODEC = select();

    private static CompressionCodec select() {
      try {
        String property = FORCE_JAVA_PROPERTY;
        String setting = System.getProperty(property);
        if (setting == null) {
          property = LEGACY_FORCE_JAVA_PROPERTY;
          setting = System.getProperty(property);
        }
        if (Boolean.parseBoolean(setting)) {
          LOG.info("Arrow LZ4 decoder: Java ({}=true)", property);
          return new OdpsLz4CompressionCodec();
        }
        CompressionCodec codec = OdpsCompressionFactory.NATIVE_LZ4_INSTANCE
            .createCodec(CodecType.LZ4_FRAME);
        LOG.info("Arrow LZ4 decoder: native LZ4F");
        return codec;
      } catch (LinkageError | SecurityException unavailable) {
        return fallback(unavailable);
      } catch (IllegalStateException unavailable) {
        // The explicit native codec wraps native linkage failures. Do not hide unrelated
        // initialization bugs, resource exhaustion, or any subsequent decompression error.
        if (!(unavailable.getCause() instanceof LinkageError)) {
          throw unavailable;
        }
        return fallback(unavailable.getCause());
      }
    }

    private static CompressionCodec fallback(Throwable unavailable) {
      LOG.warn("Arrow LZ4 decoder: Java (native initialization unavailable: {})",
          unavailable.toString());
      return new OdpsLz4CompressionCodec();
    }
  }
}
