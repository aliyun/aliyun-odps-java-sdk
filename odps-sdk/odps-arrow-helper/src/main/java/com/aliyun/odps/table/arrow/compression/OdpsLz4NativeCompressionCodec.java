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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Pointer;
import org.bytedeco.javacpp.SizeTPointer;
import org.bytedeco.lz4.LZ4FDecompressionContext;

import static org.bytedeco.lz4.global.lz4.LZ4F_VERSION;
import static org.bytedeco.lz4.global.lz4.LZ4F_compressFrame;
import static org.bytedeco.lz4.global.lz4.LZ4F_compressFrameBound;
import static org.bytedeco.lz4.global.lz4.LZ4F_createDecompressionContext;
import static org.bytedeco.lz4.global.lz4.LZ4F_decompress;
import static org.bytedeco.lz4.global.lz4.LZ4F_freeDecompressionContext;
import static org.bytedeco.lz4.global.lz4.LZ4F_getErrorName;
import static org.bytedeco.lz4.global.lz4.LZ4F_getVersion;
import static org.bytedeco.lz4.global.lz4.LZ4F_isError;

/**
 * Standard LZ4 frames encoded and decoded by the official native LZ4F implementation.
 *
 * <p>Each invocation owns its context, so a codec can be shared by concurrent readers. Arrow
 * allocators own all input/output buffers; only temporary pointers and the LZ4F context are native
 * allocations. Selecting this codec requires working native libraries and never silently falls
 * back to another implementation.
 */
public final class OdpsLz4NativeCompressionCodec extends AbstractCompressionCodec {

  private static final int PREFIX = (int) CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
  private static final int CHUNK_SIZE = 64 * 1024;

  public OdpsLz4NativeCompressionCodec() {
    try {
      LZ4F_getVersion();
    } catch (LinkageError e) {
      throw new IllegalStateException(
          "LZ4_FRAME_NATIVE requires the JavaCPP LZ4 native libraries and a writable, executable "
              + "native cache. Supported platforms: Linux x86_64, macOS x86_64/arm64. "
              + "Select LZ4_FRAME to use the Java implementation.", e);
    }
  }

  @Override
  public CompressionUtil.CodecType getCodecType() {
    return CompressionUtil.CodecType.LZ4_FRAME;
  }

  @Override
  protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf input) {
    long length = input.writerIndex();
    Preconditions.checkArgument(length <= Integer.MAX_VALUE, "LZ4 input exceeds integer limit");
    long bound = checked(LZ4F_compressFrameBound(length, null));
    Preconditions.checkArgument(bound <= Integer.MAX_VALUE - PREFIX,
        "LZ4 compressed buffer exceeds integer limit");
    ArrowBuf output = allocator.buffer(PREFIX + bound);
    boolean success = false;
    try (BytePointer source = new BytePointer(input.nioBuffer(0, (int) length));
         BytePointer destination = new BytePointer(output.nioBuffer(PREFIX, (int) bound))) {
      long written = checked(LZ4F_compressFrame(destination, bound, source, length, null));
      if (written > bound) {
        throw new IllegalStateException("LZ4F exceeded the supplied output capacity");
      }
      output.writerIndex(PREFIX + written);
      success = true;
      return output;
    } finally {
      if (!success) {
        output.close();
      }
    }
  }

  @Override
  public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf input) {
    Preconditions.checkArgument(input.writerIndex() >= PREFIX,
        "Not enough data for the Arrow compression prefix");
    long length = readUncompressedLength(input);
    Preconditions.checkArgument(length >= -1 && length <= Integer.MAX_VALUE,
        "Invalid LZ4 uncompressed length: %s", length);
    // Arrow also permits an empty buffer with only its length prefix. If a frame is supplied,
    // validate it even when the declared output is empty instead of ignoring a corrupt body.
    if (length == 0 && input.writerIndex() > PREFIX) {
      ArrowBuf output = doDecompress(allocator, input);
      input.close();
      return output;
    }
    return super.decompress(allocator, input);
  }

  @Override
  protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf input) {
    long length = readUncompressedLength(input);
    long frameLength = input.writerIndex() - PREFIX;
    Preconditions.checkArgument(frameLength > 0 && frameLength <= Integer.MAX_VALUE,
        "Invalid LZ4 frame length: %s", frameLength);
    ArrowBuf output = allocator.buffer(length);
    boolean success = false;
    try (BytePointer source = new BytePointer(input.nioBuffer(PREFIX, (int) frameLength));
         BytePointer destination = length == 0 ? new BytePointer(1L)
             : new BytePointer(output.nioBuffer(0, (int) length));
         SizeTPointer sourceSize = new SizeTPointer(1L);
         SizeTPointer destinationSize = new SizeTPointer(1L)) {
      LZ4FDecompressionContext context = new LZ4FDecompressionContext((Pointer) null);
      try {
        checked(LZ4F_createDecompressionContext(context, LZ4F_VERSION));
        long consumed = 0;
        long produced = 0;
        for (;;) {
          long availableInput = Math.min(CHUNK_SIZE, frameLength - consumed);
          long availableOutput = Math.min(CHUNK_SIZE, length - produced);
          sourceSize.put(0, availableInput);
          destinationSize.put(0, availableOutput);
          long remaining = checked(LZ4F_decompress(context, destination.position(produced),
              destinationSize, source.position(consumed), sourceSize, null));
          long used = sourceSize.get(0);
          long written = destinationSize.get(0);
          if (used < 0 || used > availableInput || written < 0 || written > availableOutput) {
            throw new IllegalStateException("LZ4F returned invalid buffer sizes");
          }
          consumed += used;
          produced += written;
          if (remaining == 0 && consumed == frameLength) {
            if (produced != length) {
              throw new IllegalArgumentException("LZ4 decoded length does not match Arrow prefix");
            }
            break;
          }
          if (used == 0 && written == 0) {
            throw new IllegalArgumentException("Incomplete LZ4 frame or incorrect Arrow length");
          }
        }
        output.writerIndex(length);
        success = true;
        return output;
      } finally {
        if (!context.isNull()) {
          LZ4F_freeDecompressionContext(context);
        }
      }
    } finally {
      if (!success) {
        output.close();
      }
    }
  }

  private static long checked(long code) {
    if (LZ4F_isError(code) != 0) {
      throw new IllegalArgumentException("LZ4F: " + LZ4F_getErrorName(code).getString());
    }
    return code;
  }
}
