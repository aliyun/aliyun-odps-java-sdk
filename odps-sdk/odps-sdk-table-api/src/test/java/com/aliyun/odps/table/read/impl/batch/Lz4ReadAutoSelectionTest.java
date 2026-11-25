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

package com.aliyun.odps.table.read.impl.batch;

import com.aliyun.odps.table.arrow.compression.OdpsCompressionFactory;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil.CodecType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Isolated JVMs are required because native loading and the automatic decision are cached. */
public class Lz4ReadAutoSelectionTest {
  private static final String FORCE_JAVA_PROPERTY = "odps.table.lz4.forceJava";
  @Test
  public void prefersNativeForReadsWithoutChangingWriterDefaults() throws Exception {
    fork("native");
  }

  @Test
  public void disabledNativeFallsBackButExplicitNativeStillFails() throws Exception {
    fork("disabled");
  }

  @Test
  public void missingBindingsFallBackButExplicitNativeStillFails() throws Exception {
    fork("missing");
  }

  @Test
  public void unsupportedPlatformFallsBackButExplicitNativeStillFails() throws Exception {
    fork("unsupported");
  }

  @Test
  public void forcedJavaOnlyAffectsAutomaticReadSelection() throws Exception {
    fork("forced");
  }

  @Test
  public void legacyJavaOverrideRemainsSupported() throws Exception {
    fork("legacy");
  }

  @Test
  public void generalOverrideTakesPrecedenceOverLegacyAlias() throws Exception {
    fork("override");
  }

  private void fork(String mode) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(new File(System.getProperty("java.home"), "bin/java").getPath());
    if (!System.getProperty("java.specification.version").startsWith("1.")) {
      command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
    }
    if (mode.equals("disabled")) {
      command.add("-Dorg.bytedeco.javacpp.loadlibraries=false");
    } else if (mode.equals("unsupported")) {
      command.add("-Dorg.bytedeco.javacpp.platform=unsupported-test-platform");
    } else if (mode.equals("forced")) {
      command.add("-D" + FORCE_JAVA_PROPERTY + "=true");
    } else if (mode.equals("legacy")) {
      command.add("-Dodps.table.v2.lz4.forceJava=true");
    } else if (mode.equals("override")) {
      command.add("-D" + FORCE_JAVA_PROPERTY + "=false");
      command.add("-Dodps.table.v2.lz4.forceJava=true");
    }
    String classpath = System.getProperty("surefire.test.class.path",
        System.getProperty("java.class.path"));
    if (mode.equals("missing")) {
      classpath = Arrays.stream(classpath.split(File.pathSeparator))
          .filter(path -> !path.contains(File.separator + "bytedeco" + File.separator))
          .collect(Collectors.joining(File.pathSeparator));
    }
    command.addAll(Arrays.asList("-cp", classpath, getClass().getName(), mode));
    File linkedFixture = File.createTempFile("odps-linked-lz4-", ".arrow");
    java.nio.file.Files.write(linkedFixture.toPath(), NativeLz4ReadPathTest.linkedStream());
    command.add(1, "-Dtest.odps.linkedLz4Stream=" + linkedFixture.getPath());
    Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
    ExecutorService outputReader = Executors.newSingleThreadExecutor();
    try {
      Future<String> output = outputReader.submit(() -> {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (InputStream input = child.getInputStream()) {
          byte[] buffer = new byte[4096];
          int read;
          while ((read = input.read(buffer)) != -1) {
            bytes.write(buffer, 0, read);
          }
        }
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
      });
      assertTrue("Timed out: " + mode, child.waitFor(60, TimeUnit.SECONDS));
      String text = output.get(5, TimeUnit.SECONDS);
      assertEquals(text, 0, child.exitValue());
      assertTrue(text, text.contains("LZ4_READ_PROBE mode=" + mode + " passed"));
    } finally {
      child.destroyForcibly();
      outputReader.shutdownNow();
      java.nio.file.Files.deleteIfExists(linkedFixture.toPath());
    }
  }

  public static void main(String[] args) {
    try {
      probe(args[0]);
    } catch (Throwable failure) {
      failure.printStackTrace();
      System.exit(1);
    }
    // SDK HTTP pools can outlive a reader. This disposable JVM tests decoder selection,
    // not their idle timeout; all readers and Arrow allocations have been closed above.
    System.exit(0);
  }

  private static void probe(String mode) throws Exception {
    boolean unavailable = mode.equals("disabled") || mode.equals("missing")
        || mode.equals("unsupported");
    String expected = mode.equals("native") || mode.equals("override")
        ? "OdpsLz4NativeCompressionCodec"
        : "OdpsLz4CompressionCodec";
    CompressionCodec.Factory factory = OdpsCompressionFactory.READ_INSTANCE;
    assertSame(factory, OdpsCompressionFactory.V2_READ_INSTANCE);
    // Non-LZ4 buffers must retain the existing implementation and compression-level handling.
    assertEquals("OdpsZstdCompressionCodec", factory.createCodec(CodecType.ZSTD).getClass()
        .getSimpleName());
    assertEquals("OdpsZstdCompressionCodec", factory.createCodec(CodecType.ZSTD, 3).getClass()
        .getSimpleName());
    ExecutorService firstReads = Executors.newFixedThreadPool(8);
    CompressionCodec selected;
    try {
      List<Future<CompressionCodec>> codecs = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        codecs.add(firstReads.submit(() -> factory.createCodec(CodecType.LZ4_FRAME)));
      }
      selected = codecs.get(0).get(10, TimeUnit.SECONDS);
      for (Future<CompressionCodec> codec : codecs) {
        assertSame(selected, codec.get(10, TimeUnit.SECONDS));
      }
    } finally {
      firstReads.shutdownNow();
    }
    assertEquals(expected, selected.getClass().getSimpleName());
    assertSame(selected, factory.createCodec(CodecType.LZ4_FRAME, 0));
    // A late property change cannot replace a decoder used by concurrent readers.
    System.setProperty(FORCE_JAVA_PROPERTY,
        Boolean.toString(mode.equals("native")));
    assertSame(selected, factory.createCodec(CodecType.LZ4_FRAME));

    assertEquals("OdpsLz4CompressionCodec", OdpsCompressionFactory.INSTANCE
        .createCodec(CodecType.LZ4_FRAME).getClass().getSimpleName());
    try {
      CompressionCodec strict = OdpsCompressionFactory.NATIVE_LZ4_INSTANCE
          .createCodec(CodecType.LZ4_FRAME);
      assertTrue("Explicit Native unexpectedly succeeded: " + mode, !unavailable);
      assertEquals("OdpsLz4NativeCompressionCodec", strict.getClass().getSimpleName());
    } catch (LinkageError | IllegalStateException failure) {
      assertTrue("Explicit Native unexpectedly failed: " + failure, unavailable);
    }
    try (RootAllocator allocator = new RootAllocator(1024 * 1024)) {
      try (ArrowBuf corrupt = allocator.buffer(16)) {
        corrupt.writeLong(65536);
        corrupt.writeLong(0);
        try {
          selected.decompress(allocator, corrupt);
          fail("Corrupt frame accepted");
        } catch (RuntimeException expectedFailure) {
          assertEquals(1, corrupt.refCnt());
        }
      }
      assertEquals(0, allocator.getAllocatedMemory());
    }
    assertSame(selected, factory.createCodec(CodecType.LZ4_FRAME));
    NativeLz4ReadPathTest paths = new NativeLz4ReadPathTest();
    paths.v2AutomaticReaderHandlesExistingCodecSettings();
    paths.v1AutomaticReaderKeepsStandardHeader();
    paths.v1Lz4ReaderHandlesLinkedFrames();
    paths.v1OtherCodecsRemainUnchanged();
    paths.v2StorageReaderHandlesLz4Streams();
    System.out.println("LZ4_READ_PROBE mode=" + mode + " passed backend=" + expected);
  }
}
