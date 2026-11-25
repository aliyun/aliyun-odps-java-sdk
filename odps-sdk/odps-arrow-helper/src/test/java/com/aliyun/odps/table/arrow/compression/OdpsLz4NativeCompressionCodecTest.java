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

import com.aliyun.odps.table.arrow.ArrowReader;
import com.aliyun.odps.table.arrow.ArrowReaderBuilder;
import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.arrow.ArrowWriterFactory;
import com.aliyun.odps.table.configuration.CompressionCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.bytedeco.javacpp.Pointer;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Offline tests only: synthetic frames and in-memory Arrow IPC, no MaxCompute credentials. */
public class OdpsLz4NativeCompressionCodecTest {
  private static final int HISTORY_LENGTH = 1024 * 1024;
  private static final String HISTORY_SHA =
      "f8d7c323f6da44578c07e42d6c14e2020c7f862cddf66e9cca69e8e7d774df2b";

  @Test
  public void selectsNativeWithoutChangingWireFormatOrDefaults() {
    assertEquals(CompressionCodec.LZ4_FRAME_NATIVE,
        CompressionCodec.byName("lz4_frame_native").get());
    assertEquals("LZ4_FRAME", CompressionCodec.LZ4_FRAME_NATIVE.getWireName());
    assertEquals("LZ4_FRAME_NATIVE", CompressionCodec.LZ4_FRAME_NATIVE.toString());
    assertEquals("ZSTD", CompressionCodec.ZSTD.getWireName());
    assertEquals("", CompressionCodec.NO_COMPRESSION.getWireName());
    assertEquals(OdpsLz4CompressionCodec.class,
        OdpsCompressionFactory.INSTANCE.createCodec(CompressionCodec.LZ4_FRAME).getClass());
    assertEquals(OdpsLz4NativeCompressionCodec.class,
        OdpsCompressionFactory.INSTANCE.createCodec(CompressionCodec.LZ4_FRAME_NATIVE).getClass());
    assertEquals(OdpsLz4NativeCompressionCodec.class,
        OdpsCompressionFactory.NATIVE_LZ4_INSTANCE
            .createCodec(CompressionUtil.CodecType.LZ4_FRAME, 0).getClass());
    assertEquals(CompressionUtil.CodecType.LZ4_FRAME,
        new OdpsLz4NativeCompressionCodec().getCodecType());
  }

  @Test
  public void nativeAndJavaCodecsReadEachOthersFrames() {
    OdpsLz4NativeCompressionCodec nativeCodec = new OdpsLz4NativeCompressionCodec();
    OdpsLz4CompressionCodec javaCodec = new OdpsLz4CompressionCodec();
    int[] lengths = {0, 1, 7, 4095, 4096, 65535, 65536, 65537, 1048576, 8388608};
    try (RootAllocator allocator = new RootAllocator(64L * 1024 * 1024)) {
      for (int length : lengths) {
        byte[] raw = data(length);
        roundTrip(allocator, raw, nativeCodec, javaCodec);
        roundTrip(allocator, raw, javaCodec, nativeCodec);
        roundTrip(allocator, raw, nativeCodec, nativeCodec);
      }
      byte[] random = new byte[65537];
      new Random(59439414).nextBytes(random);
      roundTrip(allocator, random, nativeCodec, nativeCodec); // Arrow raw-buffer sentinel.
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  public void readsActualCrossBlockHistoryFixture() throws Exception {
    byte[] frame = resource("history-linked.lz4");
    assertEquals(0, frame[4] & 32);
    OdpsLz4NativeCompressionCodec codec = new OdpsLz4NativeCompressionCodec();
    try (RootAllocator allocator = new RootAllocator(8L * 1024 * 1024)) {
      byte[] actual = decode(allocator, codec, frame, HISTORY_LENGTH);
      assertEquals(HISTORY_SHA, hex(MessageDigest.getInstance("SHA-256").digest(actual)));
      assertArrayEquals(actual,
          decode(allocator, new OdpsLz4CompressionCodec(), frame, HISTORY_LENGTH));
      byte[] minimal = decode(allocator, codec, resource("minimal-linked.lz4"), 65537);
      for (byte b : minimal) {
        assertEquals((byte) 'x', b);
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  public void rejectsDamagedFramesAndLengthsWithoutLeakingOrPoisoningTheCodec() throws Exception {
    OdpsLz4NativeCompressionCodec codec = new OdpsLz4NativeCompressionCodec();
    byte[] good = resource("history-linked.lz4");
    byte[] checksum = good.clone();
    checksum[checksum.length - 1] ^= 1;
    byte[] magic = good.clone();
    magic[0] ^= 1;
    byte[] trailing = Arrays.copyOf(good, good.length + 1);
    trailing[trailing.length - 1] = 1;
    try (RootAllocator allocator = new RootAllocator(8L * 1024 * 1024)) {
      for (byte[] bad : new byte[][] {new byte[0], Arrays.copyOf(good, good.length - 1),
          checksum, magic, trailing}) {
        reject(allocator, codec, bad, HISTORY_LENGTH);
        assertEquals(HISTORY_LENGTH, decode(allocator, codec, good, HISTORY_LENGTH).length);
      }
      for (long bad : new long[] {-2, 0, HISTORY_LENGTH - 1, HISTORY_LENGTH + 1,
          (long) Integer.MAX_VALUE + 1, Long.MAX_VALUE}) {
        reject(allocator, codec, good, bad);
      }
      try (ArrowBuf shortPrefix = allocator.buffer(8)) {
        shortPrefix.writerIndex(4);
        try {
          codec.decompress(allocator, shortPrefix);
          fail("short Arrow prefix accepted");
        } catch (IllegalArgumentException expected) {
          assertEquals(1, shortPrefix.refCnt());
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
  }

  @Test
  public void respectsAllocatorBudgetOnForgedLength() throws Exception {
    OdpsLz4NativeCompressionCodec codec = new OdpsLz4NativeCompressionCodec();
    try (RootAllocator allocator = new RootAllocator(256 * 1024);
         ArrowBuf input = framed(allocator, resource("history-linked.lz4"), HISTORY_LENGTH)) {
      long before = allocator.getAllocatedMemory();
      try {
        codec.decompress(allocator, input);
        fail("allocator limit ignored");
      } catch (org.apache.arrow.memory.OutOfMemoryException expected) {
        assertEquals(before, allocator.getAllocatedMemory());
        assertEquals(1, input.refCnt());
      }
    }
  }

  @Test
  public void acceptsArrowSlicesWithoutTouchingSurroundingMemory() throws Exception {
    byte[] frame = resource("minimal-linked.lz4");
    try (RootAllocator allocator = new RootAllocator(1024 * 1024);
         ArrowBuf parent = allocator.buffer(frame.length + 72)) {
      byte[] guards = new byte[(int) parent.capacity()];
      Arrays.fill(guards, (byte) 0x5a);
      parent.setBytes(0, guards);
      parent.setLong(32, 65537);
      parent.setBytes(40, frame);
      ArrowBuf slice = parent.slice(32, frame.length + 8);
      slice.getReferenceManager().retain();
      slice.writerIndex(frame.length + 8);
      try (ArrowBuf result = new OdpsLz4NativeCompressionCodec().decompress(allocator, slice)) {
        assertEquals(65537, result.writerIndex());
        assertEquals((byte) 'x', result.getByte(65536));
      }
      for (int i = 0; i < 32; i++) {
        assertEquals((byte) 0x5a, parent.getByte(i));
        assertEquals((byte) 0x5a, parent.getByte(40 + frame.length + i));
      }
    }
  }

  @Test
  public void concurrentCallsOwnAndReleaseTheirNativeContexts() throws Exception {
    final OdpsLz4NativeCompressionCodec codec = new OdpsLz4NativeCompressionCodec();
    final byte[] frame = resource("minimal-linked.lz4");
    final byte[] expected = new byte[65537];
    Arrays.fill(expected, (byte) 'x');
    final long before = Pointer.totalBytes();
    try (final RootAllocator allocator = new RootAllocator(64L * 1024 * 1024)) {
      ExecutorService pool = Executors.newFixedThreadPool(8);
      List<Future<Void>> jobs = new ArrayList<>();
      try {
        for (int t = 0; t < 8; t++) {
          jobs.add(pool.submit(new Callable<Void>() {
            public Void call() {
              for (int n = 0; n < 250; n++) {
                assertArrayEquals(expected, decode(allocator, codec, frame, expected.length));
              }
              return null;
            }
          }));
        }
        for (Future<Void> job : jobs) {
          job.get(30, TimeUnit.SECONDS);
        }
      } finally {
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
      }
      assertEquals(0, allocator.getAllocatedMemory());
      assertEquals(before, Pointer.totalBytes());
    }
  }

  @Test
  public void arrowFactoriesInteroperateForBothReaderOwnershipModes() throws Exception {
    try (RootAllocator allocator = new RootAllocator(32L * 1024 * 1024)) {
      for (CompressionCodec writerCodec : new CompressionCodec[] {
          CompressionCodec.LZ4_FRAME_NATIVE, CompressionCodec.LZ4_FRAME}) {
        byte[] bytes = arrowStream(allocator, writerCodec);
        for (CompressionCodec readerCodec : new CompressionCodec[] {
            CompressionCodec.LZ4_FRAME_NATIVE, CompressionCodec.LZ4_FRAME}) {
          for (boolean reuse : new boolean[] {true, false}) {
            try (ArrowReader reader = ArrowReaderBuilder.newBuilder(
                new ByteArrayInputStream(bytes), allocator).withCompression(readerCodec)
                .withReuseBatch(reuse).build()) {
              int batch = 0;
              while (reader.nextBatch()) {
                VectorSchemaRoot root = reader.getCurrentValue();
                try {
                  assertEquals(2000, root.getRowCount());
                  IntVector ids = (IntVector) root.getVector(0);
                  VarCharVector texts = (VarCharVector) root.getVector(1);
                  for (int r = 0; r < 2000; r++) {
                    assertEquals(r % 17 == 0, ids.isNull(r));
                    if (!ids.isNull(r)) {
                      assertEquals(batch * 2000 + r, ids.get(r));
                    }
                    assertEquals(r % 13 == 0, texts.isNull(r));
                    if (!texts.isNull(r)) {
                      assertArrayEquals(text(r, batch), texts.get(r));
                    }
                  }
                } finally {
                  if (!reuse) {
                    root.close();
                  }
                }
                batch++;
              }
              assertEquals(3, batch);
            }
            assertEquals(0, allocator.getAllocatedMemory());
          }
        }
      }
    }
  }

  @Test
  public void nativeUnavailabilityIsExplicitAndDoesNotInitializeLegacyPaths()
      throws Exception {
    String java = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath();
    List<String> command = new ArrayList<>();
    command.add(java);
    command.add("-Dorg.bytedeco.javacpp.loadlibraries=false");
    command.add("-Duser.language=tr");
    command.add("-Duser.country=TR");
    if (!System.getProperty("java.specification.version").equals("1.8")) {
      command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
    }
    command.add("-cp");
    command.add(System.getProperty("surefire.test.class.path",
        System.getProperty("java.class.path")));
    command.add(UnavailableProcess.class.getName());
    ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
    builder.environment().clear();
    Process process = builder.start();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      fail("native availability subprocess timed out");
    }
    byte[] output = readAll(process.getInputStream());
    assertEquals(new String(output, StandardCharsets.UTF_8), 0, process.exitValue());
  }

  public static class UnavailableProcess {
    public static void main(String[] args) {
      assertEquals(CompressionCodec.LZ4_FRAME_NATIVE,
          CompressionCodec.byName("lz4_frame_native").get());
      try (RootAllocator allocator = new RootAllocator(4L * 1024 * 1024)) {
        try {
          ArrowReaderBuilder.newBuilder(new ByteArrayInputStream(new byte[0]), allocator)
              .withCompression(null);
          throw new AssertionError("null compression silently accepted");
        } catch (NullPointerException expected) {
          // Preserve the builder's existing null-rejection contract.
        }
        roundTrip(allocator, data(65537), new OdpsLz4CompressionCodec(),
            new OdpsLz4CompressionCodec());
        try {
          OdpsCompressionFactory.INSTANCE.createCodec(CompressionCodec.LZ4_FRAME_NATIVE);
          throw new AssertionError("native selection silently fell back");
        } catch (IllegalStateException expected) {
          assertTrue(expected.getMessage().contains("LZ4_FRAME_NATIVE"));
        }
        assertEquals(0, allocator.getAllocatedMemory());
      }
    }
  }

  private static byte[] arrowStream(RootAllocator allocator, CompressionCodec codec) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (VectorSchemaRoot root = VectorSchemaRoot.of(new IntVector("id", allocator),
        new VarCharVector("text", allocator));
         ArrowWriter writer = ArrowWriterFactory.newBuilder(bytes).withCompression(codec).build()) {
      IntVector ids = (IntVector) root.getVector(0);
      VarCharVector texts = (VarCharVector) root.getVector(1);
      for (int b = 0; b < 3; b++) {
        ids.reset();
        texts.reset();
        for (int r = 0; r < 2000; r++) {
          if (r % 17 == 0) {
            ids.setNull(r);
          } else {
            ids.setSafe(r, b * 2000 + r);
          }
          if (r % 13 == 0) {
            texts.setNull(r);
          } else {
            texts.setSafe(r, text(r, b));
          }
        }
        ids.setValueCount(2000);
        texts.setValueCount(2000);
        root.setRowCount(2000);
        writer.writeBatch(root);
      }
    }
    return bytes.toByteArray();
  }

  private static byte[] text(int row, int batch) {
    return ("batch=" + batch + " 重复历史字符串 " + row % 31).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] data(int length) {
    byte[] dictionary = new byte[49152];
    new Random(59439414).nextBytes(dictionary);
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = dictionary[i % dictionary.length];
    }
    return bytes;
  }

  private static void roundTrip(RootAllocator allocator, byte[] raw,
      org.apache.arrow.vector.compression.CompressionCodec encoder,
      org.apache.arrow.vector.compression.CompressionCodec decoder) {
    ArrowBuf input = allocator.buffer(raw.length);
    input.writeBytes(raw);
    ArrowBuf encoded = encoder.compress(allocator, input);
    try (ArrowBuf decoded = decoder.decompress(allocator, encoded)) {
      assertEquals(raw.length, decoded.writerIndex());
      byte[] actual = new byte[raw.length];
      decoded.getBytes(0, actual);
      assertArrayEquals(raw, actual);
    }
  }

  private static ArrowBuf framed(RootAllocator allocator, byte[] frame, long length) {
    ArrowBuf input = allocator.buffer(frame.length + 8L);
    input.setLong(0, length);
    input.setBytes(8, frame);
    input.writerIndex(frame.length + 8L);
    return input;
  }

  private static byte[] decode(RootAllocator allocator,
      org.apache.arrow.vector.compression.CompressionCodec codec, byte[] frame, long length) {
    try (ArrowBuf result = codec.decompress(allocator, framed(allocator, frame, length))) {
      byte[] bytes = new byte[(int) result.writerIndex()];
      result.getBytes(0, bytes);
      return bytes;
    }
  }

  private static void reject(RootAllocator allocator, OdpsLz4NativeCompressionCodec codec,
      byte[] frame, long length) {
    try (ArrowBuf input = framed(allocator, frame, length)) {
      long before = allocator.getAllocatedMemory();
      try (ArrowBuf ignored = codec.decompress(allocator, input)) {
        fail("bad frame/length accepted: " + length);
      } catch (IllegalArgumentException expected) {
        assertEquals(before, allocator.getAllocatedMemory());
        assertEquals(1, input.refCnt());
      }
    }
  }

  private static byte[] resource(String name) throws Exception {
    try (InputStream input = OdpsLz4NativeCompressionCodecTest.class
        .getResourceAsStream("/lz4-native/" + name)) {
      assertNotNull(input);
      return readAll(input);
    }
  }

  private static byte[] readAll(InputStream input) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    byte[] chunk = new byte[4096];
    for (int n; (n = input.read(chunk)) != -1;) {
      bytes.write(chunk, 0, n);
    }
    return bytes.toByteArray();
  }

  private static String hex(byte[] bytes) {
    StringBuilder hex = new StringBuilder();
    for (byte b : bytes) {
      hex.append(String.format("%02x", b & 255));
    }
    return hex.toString();
  }
}
