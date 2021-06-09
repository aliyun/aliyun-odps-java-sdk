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

package com.aliyun.odps.tunnel.io;

import com.google.protobuf.CodedInputStream;
import org.xerial.snappy.PureJavaCrc32C;
import org.xerial.snappy.SnappyFramedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.zip.InflaterInputStream;

public class ArrowHttpInputStream implements ReadableByteChannel {

    private InputStream in;
    private CompressOption compress;
    private byte buf[] = new byte[0];

    private PureJavaCrc32C chunkCrc = new PureJavaCrc32C();
    private PureJavaCrc32C globalCrc = new PureJavaCrc32C();
    private int crcChunkSize;
    private int readPos; //can read pos
    private int bufSize; //buf size
    private int bufCapacity;
    private boolean isOpen;
    private boolean isLastChunk;

    private static int bytesToInt(byte[] bytes) {
        return ((bytes[0] & 255) << 24) + ((bytes[1] & 255) << 16) + ((bytes[2] & 255) << 8) + (bytes[3] & 255);
    }

    private static int bytesToInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 255) << 24) + ((bytes[1 + offset] & 255) << 16) + ((bytes[2 + offset] & 255) << 8) + (bytes[3 + offset] & 255);
    }

    public ArrowHttpInputStream(InputStream inputStream, CompressOption compress) throws IOException {
        this.in = inputStream;
        this.bufSize = 0;
        this.readPos = 0;
        this.isOpen = true;
        this.isLastChunk = false;
        this.compress = compress;
        if (compress != null && !compress.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_RAW)) {
            if (compress.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_ZLIB)) {
                this.in = new InflaterInputStream(inputStream);
            } else if (compress.algorithm.equals(CompressOption.CompressAlgorithm.ODPS_SNAPPY)) {
                this.in = new SnappyFramedInputStream(inputStream);
            } else {
                throw new IOException("invalid compression option.");
            }
        }
    }

    private boolean readChunk() throws IOException {
        if (bufSize > readPos) {
            return true;
        }
        if (buf.length == 0) {
            byte[] temp = new byte[4];
            if (in.read(temp) != -1) {
                crcChunkSize = bytesToInt(temp);
                bufCapacity = crcChunkSize + 4;
                buf = new byte[bufCapacity];
            }
        }
        bufSize = 0;
        int byteRead = -1;
        while ((bufSize < bufCapacity) && (byteRead = in.read(buf, bufSize, bufCapacity - bufSize)) != -1) {
            bufSize += byteRead;
        }
        if (bufSize == 0) {
            return false;
        }
        if (bufSize < 4) {
            throw new IOException("InputStream Read() for crc32 failed.");
        }
        int dataSize = bufSize - 4;
        int readCheckSum = bytesToInt(buf, dataSize);
        int checkSum = 0;
        globalCrc.update(buf, 0, dataSize);
        if (bufSize < crcChunkSize + 4) {
            isLastChunk = true;
            checkSum = (int) globalCrc.getValue();
        } else {
            chunkCrc.reset();
            chunkCrc.update(buf, 0, dataSize);
            checkSum = (int) chunkCrc.getValue();
        }
        if (checkSum != readCheckSum) {
            throw new IOException("CRC Check failed.");
        }
        bufSize = dataSize;
        readPos = 0;
        return true;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!isOpen)
            throw new IOException("Operation forbidden on closed BufferReader");
        int len = dst.remaining();
        int totalRead;
        int readBytes;
        for(totalRead = 0; totalRead < len && this.readChunk(); totalRead += readBytes) {
            readBytes = Math.min(this.bufSize - this.readPos, len - totalRead);
            dst.put(this.buf, this.readPos, readBytes);
            this.readPos += readBytes;
        }
        return totalRead;
    }

    @Override
    public boolean isOpen() {
        return this.isOpen;
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            this.isOpen = false;
            this.in.close();
        }
    }
}
