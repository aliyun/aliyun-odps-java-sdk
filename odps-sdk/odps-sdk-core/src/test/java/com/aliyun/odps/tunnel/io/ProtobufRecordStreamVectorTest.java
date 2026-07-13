package com.aliyun.odps.tunnel.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamReader;
import com.aliyun.odps.commons.proto.ProtobufRecordStreamWriter;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.DoubleVector;
import com.aliyun.odps.data.FloatVector;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.Vector;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * Tests for Vector type serialization/deserialization through the Tunnel protobuf protocol.
 */
public class ProtobufRecordStreamVectorTest {

  /**
   * Round-trip test for FloatVector: write → read → verify values match.
   */
  @Test
  public void testFloatVectorRoundTrip() throws IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("vec", TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 4)));

    FloatVector original = FloatVector.of(1.0f, 2.5f, -3.14f, 0.0f);

    byte[] bytes = writeRecord(schema, record -> record.setVector(0, original));
    Record result = readRecord(schema, bytes);

    Vector readVec = (Vector) result.get(0);
    Assert.assertNotNull(readVec);
    Assert.assertTrue(readVec instanceof FloatVector);
    Assert.assertEquals(OdpsType.FLOAT, readVec.elementType());
    Assert.assertEquals(4, readVec.dimension());
    Assert.assertArrayEquals(original.toFloatArray(), readVec.toFloatArray(), 0.0f);
  }

  /**
   * Round-trip test for DoubleVector: write → read → verify values match.
   */
  @Test
  public void testDoubleVectorRoundTrip() throws IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("vec", TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.DOUBLE, 3)));

    DoubleVector original = DoubleVector.of(1.0, -2.718281828, 3.141592653589793);

    byte[] bytes = writeRecord(schema, record -> record.setVector(0, original));
    Record result = readRecord(schema, bytes);

    Vector readVec = (Vector) result.get(0);
    Assert.assertNotNull(readVec);
    Assert.assertTrue(readVec instanceof DoubleVector);
    Assert.assertEquals(OdpsType.DOUBLE, readVec.elementType());
    Assert.assertEquals(3, readVec.dimension());
    Assert.assertArrayEquals(original.toDoubleArray(), readVec.toDoubleArray(), 0.0);
  }

  /**
   * Test null Vector value round-trip.
   */
  @Test
  public void testNullVectorRoundTrip() throws IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("vec", TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 2)));

    byte[] bytes = writeRecord(schema, record -> {
      // leave the vector column as null (default)
    });
    Record result = readRecord(schema, bytes);

    Assert.assertNull(result.get(0));
  }

  /**
   * Test schema with mixed types including Vector.
   */
  @Test
  public void testMixedSchemaWithVector() throws IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("id", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("name", TypeInfoFactory.STRING));
    schema.addColumn(new Column("float_vec", TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 3)));
    schema.addColumn(new Column("double_vec", TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.DOUBLE, 2)));

    FloatVector fvec = FloatVector.of(0.1f, 0.2f, 0.3f);
    DoubleVector dvec = DoubleVector.of(1.11, 2.22);

    byte[] bytes = writeRecord(schema, record -> {
      record.setBigint(0, 42L);
      record.setString(1, "hello");
      record.setVector(2, fvec);
      record.setVector(3, dvec);
    });
    Record result = readRecord(schema, bytes);

    Assert.assertEquals(42L, result.get(0));
    // String is read back as byte[]
    Assert.assertEquals("hello", new String((byte[]) result.get(1), "UTF-8"));

    Vector rfvec = (Vector) result.get(2);
    Assert.assertTrue(rfvec instanceof FloatVector);
    Assert.assertArrayEquals(fvec.toFloatArray(), rfvec.toFloatArray(), 0.0f);

    Vector rdvec = (Vector) result.get(3);
    Assert.assertTrue(rdvec instanceof DoubleVector);
    Assert.assertArrayEquals(dvec.toDoubleArray(), rdvec.toDoubleArray(), 0.0);
  }

  /**
   * Test multiple records with Vector columns.
   */
  @Test
  public void testMultipleRecordsWithVector() throws IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("vec", TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 2)));

    FloatVector vec1 = FloatVector.of(1.0f, 2.0f);
    FloatVector vec2 = FloatVector.of(3.0f, 4.0f);
    FloatVector vec3 = FloatVector.of(5.0f, 6.0f);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressOption rawOption = new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
    ProtobufRecordStreamWriter writer = new ProtobufRecordStreamWriter(schema, baos, rawOption);

    ArrayRecord record = new ArrayRecord(schema.getColumns().toArray(new Column[0]));

    record.setVector(0, vec1);
    writer.write(record);

    record.setVector(0, vec2);
    writer.write(record);

    record.setVector(0, vec3);
    writer.write(record);

    writer.close();

    byte[] data = baos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    ProtobufRecordStreamReader reader = new ProtobufRecordStreamReader(schema, bais, rawOption);

    Record r1 = reader.read();
    Assert.assertNotNull(r1);
    Assert.assertArrayEquals(vec1.toFloatArray(), ((Vector) r1.get(0)).toFloatArray(), 0.0f);

    Record r2 = reader.read();
    Assert.assertNotNull(r2);
    Assert.assertArrayEquals(vec2.toFloatArray(), ((Vector) r2.get(0)).toFloatArray(), 0.0f);

    Record r3 = reader.read();
    Assert.assertNotNull(r3);
    Assert.assertArrayEquals(vec3.toFloatArray(), ((Vector) r3.get(0)).toFloatArray(), 0.0f);

    Assert.assertNull(reader.read());
    reader.close();
  }

  /**
   * Test high-dimensional Vector (typical embedding sizes like 1536).
   */
  @Test
  public void testHighDimensionalFloatVector() throws IOException {
    int dim = 1536;
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("embedding",
        TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, dim)));

    float[] values = new float[dim];
    for (int i = 0; i < dim; i++) {
      values[i] = (float) Math.sin(i * 0.01);
    }
    FloatVector original = FloatVector.of(values);

    byte[] bytes = writeRecord(schema, record -> record.setVector(0, original));
    Record result = readRecord(schema, bytes);

    Vector readVec = (Vector) result.get(0);
    Assert.assertEquals(dim, readVec.dimension());
    Assert.assertArrayEquals(original.toFloatArray(), readVec.toFloatArray(), 0.0f);
  }

  /**
   * Test high-dimensional DoubleVector.
   */
  @Test
  public void testHighDimensionalDoubleVector() throws IOException {
    int dim = 768;
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("embedding",
        TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.DOUBLE, dim)));

    double[] values = new double[dim];
    for (int i = 0; i < dim; i++) {
      values[i] = Math.cos(i * 0.01);
    }
    DoubleVector original = DoubleVector.of(values);

    byte[] bytes = writeRecord(schema, record -> record.setVector(0, original));
    Record result = readRecord(schema, bytes);

    Vector readVec = (Vector) result.get(0);
    Assert.assertEquals(dim, readVec.dimension());
    Assert.assertArrayEquals(original.toDoubleArray(), readVec.toDoubleArray(), 0.0);
  }

  /**
   * Test single-dimension Vector (edge case).
   */
  @Test
  public void testSingleDimensionVector() throws IOException {
    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("vec", TypeInfoFactory.getVectorTypeInfo(TypeInfoFactory.FLOAT, 1)));

    FloatVector original = FloatVector.of(42.0f);

    byte[] bytes = writeRecord(schema, record -> record.setVector(0, original));
    Record result = readRecord(schema, bytes);

    Vector readVec = (Vector) result.get(0);
    Assert.assertEquals(1, readVec.dimension());
    Assert.assertArrayEquals(new float[]{42.0f}, readVec.toFloatArray(), 0.0f);
  }

  // ----- helpers -----

  @FunctionalInterface
  interface RecordPopulator {
    void populate(ArrayRecord record) throws IOException;
  }

  private byte[] writeRecord(TableSchema schema, RecordPopulator populator) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressOption rawOption = new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
    ProtobufRecordStreamWriter writer = new ProtobufRecordStreamWriter(schema, baos, rawOption);

    ArrayRecord record = new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    populator.populate(record);
    writer.write(record);
    writer.close();

    return baos.toByteArray();
  }

  private Record readRecord(TableSchema schema, byte[] data) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    CompressOption rawOption = new CompressOption(CompressOption.CompressAlgorithm.ODPS_RAW, 0, 0);
    ProtobufRecordStreamReader reader = new ProtobufRecordStreamReader(schema, bais, rawOption);

    Record result = reader.read();
    reader.close();
    return result;
  }
}
