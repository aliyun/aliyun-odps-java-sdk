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
import static org.junit.Assert.fail;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.Vector;
import com.aliyun.odps.exceptions.SchemaMismatchRuntimeException;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

/**
 * Boundary tests for the VECTOR dimension read off the wire. Fully offline: the frames are built
 * here, so no tunnel endpoint, credentials or MaxCompute project is involved.
 */
public class ProtobufRecordStreamVectorDimensionTest {

  private static CompressOption raw() {
    return new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
  }

  /** Same framing as ProtobufRecordStreamWriter: tag, dimension, elements, record end and CRC. */
  private static byte[] frame(int wireDimension, float... elements) throws IOException {
    Checksum crc = new Checksum();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    CodedOutputStream out = CodedOutputStream.newInstance(bytes);
    crc.update(1);
    out.writeTag(1, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeInt32NoTag(wireDimension);
    for (float element : elements) {
      crc.update(element);
      out.writeFloatNoTag(element);
    }
    out.writeUInt32(ProtoWireConstant.TUNNEL_END_RECORD, (int) crc.getValue());
    out.flush();
    return bytes.toByteArray();
  }

  private static Record read(int declaredDimension, byte[] frame) throws IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(
        new Column("vec", TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, declaredDimension)));
    ProtobufRecordStreamReader reader =
        new ProtobufRecordStreamReader(schema, new ByteArrayInputStream(frame), raw());
    try {
      return reader.read();
    } finally {
      reader.close();
    }
  }

  @Test
  public void matchingDimensionIsRead() throws Exception {
    Record record = read(2, frame(2, 1.5f, -2.5f));
    Vector vector = (Vector) record.get(0);
    assertEquals(2, vector.dimension());
    assertEquals(1.5f, (Float) vector.getElement(0), 0.0f);
    assertEquals(-2.5f, (Float) vector.getElement(1), 0.0f);
  }

  /**
   * A dimension that does not match the schema has to be reported as the same schema mismatch
   * ArrayRecord.set() reported -- but before the elements are buffered, because the count comes off
   * the wire.
   */
  @Test
  public void largerWireDimensionFailsAsSchemaMismatch() throws Exception {
    try {
      read(2, frame(3, 1f, 2f, 3f));
      fail("expected SchemaMismatchRuntimeException");
    } catch (SchemaMismatchRuntimeException e) {
      assertEquals("Vector dimension mismatch: expected 2, but got 3", e.getMessage());
    }
  }

  /**
   * The old code sized an ArrayList from the wire value first: a zero length was rejected by a
   * downstream constructor check with an IllegalArgumentException, and a huge one was never rejected
   * at all before the allocation blew the heap (see evidence/verification-log-2026-09-29.txt).
   */
  @Test
  public void zeroWireDimensionFailsAsSchemaMismatch() throws Exception {
    try {
      read(2, frame(0));
      fail("expected SchemaMismatchRuntimeException");
    } catch (SchemaMismatchRuntimeException e) {
      assertEquals("Vector dimension mismatch: expected 2, but got 0", e.getMessage());
    }
  }

  @Test
  public void hugeWireDimensionIsRejectedWithoutAllocating() throws Exception {
    try {
      read(1, frame(1 << 30, 1f));
      fail("expected SchemaMismatchRuntimeException");
    } catch (SchemaMismatchRuntimeException e) {
      assertEquals("Vector dimension mismatch: expected 1, but got 1073741824", e.getMessage());
    }
  }
}
