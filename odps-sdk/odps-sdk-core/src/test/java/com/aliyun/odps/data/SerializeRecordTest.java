package com.aliyun.odps.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SerializeRecordTest {

  public static TableSchema schema;

  @BeforeClass
  public static void init() {
    schema = new TableSchema();
    schema.addColumn(new Column("char", TypeInfoFactory.getCharTypeInfo(5)));
    schema.addColumn(new Column("varchar", TypeInfoFactory.getVarcharTypeInfo(10)));
    schema.addColumn(Column.newBuilder("string", TypeInfoFactory.STRING).notNull().build());
    schema.addColumn(new Column("boolean", TypeInfoFactory.BOOLEAN));
    schema.addColumn(new Column("binary", TypeInfoFactory.BINARY));
    schema.addColumn(
        new Column("decimal", TypeInfoFactory.getDecimalTypeInfo(10, 2)));
    schema.addColumn(new Column("tinyint", TypeInfoFactory.TINYINT));
    schema.addColumn(new Column("smallint", TypeInfoFactory.SMALLINT));
    schema.addColumn(new Column("int", TypeInfoFactory.INT));
    schema.addColumn(new Column("bigint", TypeInfoFactory.BIGINT));
    schema.addColumn(new Column("float", TypeInfoFactory.FLOAT));
    schema.addColumn(new Column("double", TypeInfoFactory.DOUBLE));
    schema.addColumn(new Column("date", TypeInfoFactory.DATE));
    schema.addColumn(new Column("datetime", TypeInfoFactory.DATETIME));
    schema.addColumn(new Column("timestamp_ntz", TypeInfoFactory.TIMESTAMP_NTZ));
    schema.addColumn(new Column("timestamp", TypeInfoFactory.TIMESTAMP));
    schema.addColumn(
        new Column(
            "array",
            TypeInfoFactory.getArrayTypeInfo(
                TypeInfoFactory.INT)));
    schema.addColumn(
        new Column(
            "map",
            TypeInfoFactory.getMapTypeInfo(
                TypeInfoFactory.INT,
                TypeInfoFactory.INT)));
    schema.addColumn(
        new Column(
            "row",
            TypeInfoFactory.getStructTypeInfo(
                ImmutableList.of("f0", "f1", "f2"),
                ImmutableList.of(
                    TypeInfoFactory.INT,
                    TypeInfoFactory.INT,
                    TypeInfoFactory.INT))));
  }

  @Test
  public void testSerializeSchema() {
    String str = serializeObjectToString(schema);
    TableSchema tableSchema = (TableSchema) deserializeStringToObject(str);
    Assert.assertEquals(schema, tableSchema);
  }


  @Test
  public void testUploadData() throws OdpsException, IOException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.decimal.odps2", "true");
    hints.put("odps.sql.type.system.odps2", "true");
    odps.tables().delete("test_serialize_record", true);
    odps.tables().newTableCreator(odps.getDefaultProject(), "test_serialize_record", schema)
        .ifNotExists().withHints(hints).create();

    TableTunnel tunnel = new TableTunnel(odps);
    TableTunnel.UploadSession
        uploadSession =
        tunnel.createUploadSession(odps.getDefaultProject(), "test_serialize_record", true);
    ArrayRecord record = (ArrayRecord) uploadSession.newRecord();
    fillRecord(record);

    String str = serializeObjectToString(record);
    ArrayRecord arrayRecord = (ArrayRecord) deserializeStringToObject(str);

    RecordWriter recordWriter = uploadSession.openBufferedWriter();
    recordWriter.write(arrayRecord);
    recordWriter.close();
    uploadSession.commit();

    String
        strRecord =
        odps.tables().get("test_serialize_record").read(1).stream().map(Object::toString).collect(
            Collectors.joining(", "));
    System.out.println(strRecord);
  }

  @Test
  public void testUpsertData() throws OdpsException, IOException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    Map<String, String> hints = new HashMap<>();
    hints.put("odps.sql.decimal.odps2", "true");
    hints.put("odps.sql.type.system.odps2", "true");
    odps.tables().delete("test_serialize_record2", true);
    odps.tables().newTableCreator(odps.getDefaultProject(), "test_serialize_record2", schema)
        .ifNotExists().withHints(hints).withPrimaryKeys(ImmutableList.of("string"))
        .transactionTable().create();

    TableTunnel tunnel = new TableTunnel(odps);
    TableTunnel.UpsertSession
        session =
        tunnel.buildUpsertSession(odps.getDefaultProject(), "test_serialize_record2").build();
    ArrayRecord record = (ArrayRecord) session.newRecord();
    fillRecord(record);

    String str = serializeObjectToString(record);
    ArrayRecord arrayRecord = (ArrayRecord) deserializeStringToObject(str);

    UpsertStream stream = session.buildUpsertStream().build();
    stream.upsert(arrayRecord);
    stream.close();
    session.commit(false);

    String
        strRecord =
        odps.tables().get("test_serialize_record2").read(1).stream().map(Object::toString).collect(
            Collectors.joining(", "));
    System.out.println(strRecord);
  }

  private void fillRecord(ArrayRecord record) {
    record.setChar(0, new Char("12345"));
    record.setVarchar(1, new Varchar("1234567890"));
    record.setString(2, "1234567890");
    record.setBoolean(3, true);
    record.setBinary(4, new Binary("12345".getBytes()));
    record.setDecimal(5, new BigDecimal("12345.67"));
    record.setTinyint(6, (byte) 1);
    record.setSmallint(7, (short) 1);
    record.setInt(8, 1);
    record.setBigint(9, 1L);
    record.setFloat(10, 1.0f);
    record.setDouble(11, 1.0);
    record.setDateAsLocalDate(12, LocalDate.ofEpochDay(0));
    record.setDatetimeAsZonedDateTime(13, ZonedDateTime.ofInstant(Instant.ofEpochSecond(0),
                                                                  ZoneId.of("UTC")));
    record.setTimestampNtz(14, LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC));
    record.setTimestampAsInstant(15, Instant.ofEpochSecond(0));
    record.setArray(16, ImmutableList.of(1, 2, 3));
    record.setMap(17, ImmutableMap.of(1, 3));
    record.setStruct(18, new SimpleStruct(TypeInfoFactory.getStructTypeInfo(
        ImmutableList.of("f0", "f1", "f2"),
        ImmutableList.of(
            TypeInfoFactory.INT,
            TypeInfoFactory.INT,
            TypeInfoFactory.INT)), ImmutableList.of(1, 3, 4)));
  }


  public static String serializeObjectToString(Serializable object) {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {

      objectOutputStream.writeObject(object);
      objectOutputStream.flush();
      return Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray());

    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static Object deserializeStringToObject(String string) {
    byte[] data = Base64.getDecoder().decode(string);
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
         ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {

      return objectInputStream.readObject();

    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }
}
