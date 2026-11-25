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

package com.aliyun.odps.commons.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.StreamTruncatedException;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.Test;

/**
 * Offline boundary tests for how the tunnel record stream ends. Frames are built here with the same
 * framing the writer uses (tag, payload, per-record CRC, then META_COUNT + stream CRC), so a stream
 * that stops early -- the shape a truncated download looks like -- can be fed to the real reader
 * without any endpoint or credentials.
 */
public class ProtobufRecordStreamTruncationTest {

  private static final Column[] COLUMNS =
      new Column[] {new Column("s", TypeInfoFactory.STRING), new Column("n", TypeInfoFactory.BIGINT)};

  private static CompressOption raw() {
    return new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
  }

  /** One record: crc(field idx) -> tag/len/payload -> crc(value) ... then the record end tag. */
  private static void writeRecord(Checksum recordCrc, CodedOutputStream out, String s, long n)
      throws IOException {
    byte[] payload = s.getBytes(StandardCharsets.UTF_8);
    recordCrc.update(1);
    out.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    recordCrc.update(payload, 0, payload.length);
    out.writeRawVarint32(payload.length);
    out.writeRawBytes(payload);
    recordCrc.update(2);
    out.writeTag(2, WireFormat.WIRETYPE_VARINT);
    recordCrc.update(n);
    out.writeSInt64NoTag(n);   // BIGINT goes out zigzag, matching the reader's readSInt64
    out.writeUInt32(ProtoWireConstant.TUNNEL_END_RECORD, (int) recordCrc.getValue());
  }

  /** The stream footer: record count plus the CRC-of-CRCs the reader verifies on the last read. */
  private static void writeFooter(CodedOutputStream out, int records, Checksum streamCrc,
                                  boolean withChecksumTag) throws IOException {
    out.writeSInt64(ProtoWireConstant.TUNNEL_META_COUNT, records);
    if (withChecksumTag) {
      out.writeUInt32(ProtoWireConstant.TUNNEL_META_CHECKSUM, (int) streamCrc.getValue());
    }
  }

  private static byte[] stream(boolean withFooter, int records) throws IOException {
    return stream(withFooter, records, true);
  }

  private static byte[] stream(boolean withFooter, int records, boolean withChecksumTag)
      throws IOException {
    Checksum streamCrc = new Checksum();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(bytes);
    for (int i = 0; i < records; i++) {
      Checksum recordCrc = new Checksum();
      writeRecord(recordCrc, out, "row-" + i, i);
      streamCrc.update((int) recordCrc.getValue());
    }
    // One CodedOutputStream for the whole stream: a second one over the same
    // ByteArrayOutputStream would buffer its footer bytes and never flush them, and the
    // reader would then see a footer-less stream even though we meant to write one.
    if (withFooter) {
      writeFooter(out, records, streamCrc, withChecksumTag);
    }
    out.flush();
    return bytes.toByteArray();
  }

  private static ProtobufRecordStreamReader reader(byte[] data, boolean requireStreamFooter)
      throws IOException {
    TableSchema schema = new TableSchema();
    for (Column column : COLUMNS) {
      schema.addColumn(column);
    }
    ProtobufRecordStreamReader reader = new ProtobufRecordStreamReader(
        schema, Arrays.asList(COLUMNS), new ByteArrayInputStream(data), raw());
    reader.setRequireStreamFooter(requireStreamFooter);
    return reader;
  }

  private static int drain(ProtobufRecordStreamReader reader) throws IOException {
    int read = 0;
    Record record;
    ArrayRecord reuse = new ArrayRecord(COLUMNS, false);
    while ((record = reader.read(reuse)) != null) {
      assertEquals("row-" + read, new String((byte[]) record.get(0), StandardCharsets.UTF_8));
      read++;
    }
    return read;
  }

  @Test
  public void wellFormedStreamWithFooterReadsEverything() throws Exception {
    try (ProtobufRecordStreamReader reader = reader(stream(true, 3), true)) {
      assertEquals(3, drain(reader));
    }
  }

  /**
   * A stream that ends right after the last record -- the shape a truncated download leaves behind
   * -- must not be reported as a complete read once the footer is required.
   */
  @Test
  public void missingFooterIsSilentByDefault() throws Exception {
    try (ProtobufRecordStreamReader reader = reader(stream(false, 3), false)) {
      assertEquals("without the footer requirement an early end is indistinguishable from EOF",
                   3, drain(reader));
    }
  }

  @Test
  public void missingFooterThrowsWhenRequired() throws Exception {
    try (ProtobufRecordStreamReader reader = reader(stream(false, 3), true)) {
      drain(reader);
      fail("expected StreamTruncatedException when the footer is required but absent");
    } catch (StreamTruncatedException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("footer tag"));
      assertTrue(e.getMessage(), e.getMessage().contains("Records read so far: 3"));
    }
  }

  /** A record cut in the middle is a read error either way; it must never look like a clean end. */
  @Test
  public void halfWrittenRecordIsAnError() throws Exception {
    byte[] whole = stream(false, 3);
    byte[] cut = Arrays.copyOf(whole, whole.length - 3);
    for (boolean requireFooter : new boolean[] {false, true}) {
      try (ProtobufRecordStreamReader reader = reader(cut, requireFooter)) {
        drain(reader);
        fail("a truncated record must not read as success (requireStreamFooter=" + requireFooter + ")");
      } catch (IOException e) {
        // Measured shape, same in both modes: protobuf reports a record that stops in the middle
        // as InvalidProtocolBufferException (an IOException), so it surfaces through the reader's
        // declared contract instead of looking like a clean end of stream.
        assertTrue("requireStreamFooter=" + requireFooter + " got " + e.getClass().getName(),
                   e instanceof InvalidProtocolBufferException);
        assertTrue(e.getMessage(), e.getMessage().contains("truncated"));
      }
    }
  }

  /** Empty stream: with the footer required this is a truncation too, not "zero rows". */
  @Test
  public void emptyStreamWithoutFooterIsSilent() throws Exception {
    try (ProtobufRecordStreamReader reader = reader(new byte[0], false)) {
      assertNull(reader.read());
    }
    try (ProtobufRecordStreamReader reader = reader(new byte[0], true)) {
      drain(reader);
      fail("expected StreamTruncatedException for a footerless empty stream");
    } catch (StreamTruncatedException expected) {
      assertTrue(expected.getMessage(), expected.getMessage().contains("Records read so far: 0"));
    }
  }

  /**
   * Anti-vacuity for the two footer cases above: a byte flipped inside a record payload has to be
   * caught by the per-record CRC, otherwise "the reader reached the footer" would say nothing about
   * the bytes in between. Measured: the reader reports it as IOException("Checksum invalid.").
   */
  @Test
  public void aFlippedByteInsideARecordIsDetected() throws Exception {
    byte[] whole = stream(true, 2);
    int payload = indexOf(whole, "row-1");
    whole[payload + 1] ^= 0x01;
    try (ProtobufRecordStreamReader reader = reader(whole, true)) {
      drain(reader);
      fail("a corrupted record byte must not read as success");
    } catch (IOException e) {
      assertEquals(IOException.class, e.getClass());
      assertTrue(e.getMessage(), e.getMessage().contains("Checksum invalid"));
    }
  }

  /** The footer CRC-of-CRCs is really compared: a wrong value is an error, not a formality. */
  @Test
  public void aWrongFooterChecksumIsDetected() throws Exception {
    byte[] whole = stream(true, 2);
    // -2, not -1: the checksum goes out as a 5-byte uint32 varint and the reader's readUInt32 keeps
    // only the low 32 bits, so flipping the top byte (bit 28..34 of the value) leaves the compared
    // value unchanged (measured with a frame-corruption probe: flipping the last byte reads 2 rows
    // with no exception, flipping the one before it raises "Checksum invalid.").
    whole[whole.length - 2] ^= 0x40;
    try (ProtobufRecordStreamReader reader = reader(whole, true)) {
      drain(reader);
      fail("a wrong footer checksum must not read as success");
    } catch (IOException e) {
      assertEquals(IOException.class, e.getClass());
      assertTrue(e.getMessage(), e.getMessage().contains("Checksum invalid"));
    }
  }

  /**
   * What `requireStreamFooter` actually requires: the META_COUNT tag plus a row count that matches
   * what was read. The CRC-of-CRCs is only compared when its own tag is present, so a stream cut
   * right after the count varint is still reported as complete (every record had already passed its
   * own CRC check, so no row is lost -- it is the extra stream-level checksum that gets skipped).
   * Pinned so that tightening this later is a deliberate change rather than an accident.
   */
  @Test
  public void footerWithoutTheChecksumTagIsAcceptedAsComplete() throws Exception {
    byte[] countOnly = stream(true, 3, false);
    try (ProtobufRecordStreamReader reader = reader(countOnly, true)) {
      assertEquals("the count tag alone satisfies requireStreamFooter", 3, drain(reader));
    }
  }

  private static int indexOf(byte[] data, String needle) {
    byte[] n = needle.getBytes(StandardCharsets.UTF_8);
    outer:
    for (int i = 0; i + n.length <= data.length; i++) {
      for (int j = 0; j < n.length; j++) {
        if (data[i + j] != n[j]) {
          continue outer;
        }
      }
      return i;
    }
    throw new IllegalStateException("payload marker not found in the frame: " + needle);
  }
}
