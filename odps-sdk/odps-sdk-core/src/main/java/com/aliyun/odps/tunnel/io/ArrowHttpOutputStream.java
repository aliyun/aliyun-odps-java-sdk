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

import org.xerial.snappy.PureJavaCrc32C;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ArrowHttpOutputStream implements WritableByteChannel {

    private OutputStream out;

    private byte buf[] = new byte[0];
    private final int CHUNK_SIZE;
    private PureJavaCrc32C chunkCrc = new PureJavaCrc32C();
    private PureJavaCrc32C globalCrc = new PureJavaCrc32C();
    private boolean isOpen;
    private int currentPosition;
    private boolean isWriteChunkSize = false;

    public ArrowHttpOutputStream(OutputStream outputStream) {
        this(outputStream, 65536);
    }

    public ArrowHttpOutputStream(OutputStream outputStream, int chunkSize) {
        this.out = outputStream;
        this.isOpen = true;
        this.CHUNK_SIZE = chunkSize;
    }

    private void writeChunk(ByteBuffer src, int length) throws IOException {
        src.get(buf, currentPosition, length);
        out.write(buf, currentPosition, length);
        chunkCrc.update(buf, currentPosition, length);
        globalCrc.update(buf, currentPosition, length);
        currentPosition += length;

        if (currentPosition >= CHUNK_SIZE) {
            int crcCheckSum = (int) chunkCrc.getValue();
            writeUInt32(crcCheckSum);
            chunkCrc.reset();
            currentPosition = 0;
        }
    }

    private void writeUInt32(int number) throws IOException {
        byte[] temp = new byte[4];
        intToBytes(number, temp);
        out.write(temp, 0, 4);
    }

    private void flush() throws IOException {
        int globalCrcCheckSum = (int) globalCrc.getValue();
        writeUInt32(globalCrcCheckSum);
        globalCrc.reset();
    }

    private static void intToBytes(int value, byte[] bytes) {
        bytes[0] = (byte) (value >>> 24);
        bytes[1] = (byte) (value >>> 16);
        bytes[2] = (byte) (value >>> 8);
        bytes[3] = (byte) value;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!isOpen)
            throw new IOException("Operation forbidden on closed BufferReader");
        if (!isWriteChunkSize) {
            writeUInt32(CHUNK_SIZE);
            buf = new byte[CHUNK_SIZE];
            isWriteChunkSize = true;
        }
        int len = src.remaining();
        int totalWrite;
        int writeBytes;
        for(totalWrite = 0; totalWrite < len; totalWrite += writeBytes) {
            writeBytes = Math.min(this.CHUNK_SIZE - this.currentPosition, len - totalWrite);
            this.writeChunk(src, writeBytes);
        }
        return totalWrite;
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() throws IOException {
        if (isOpen) {
            this.flush();
            this.isOpen = false;
            this.out.close();
        }
    }
}
