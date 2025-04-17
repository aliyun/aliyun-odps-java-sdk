package com.aliyun.odps.utils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.TimeZone;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CSVRecordParserTest {

  private static final String tableName = "all_type_csv_record_parser_test";
  private static final TableSchema allTypeSchema = new TableSchema();
  private static final Odps odps = OdpsTestUtils.newDefaultOdps();

  @BeforeClass
  public static void createTable() throws Exception {
    // Step 1: 创建包含所有 ODPS 类型的 TableSchema
    allTypeSchema.addColumn(new Column("bigint_col", TypeInfoFactory.BIGINT));
    allTypeSchema.addColumn(new Column("double_col", TypeInfoFactory.DOUBLE));
    allTypeSchema.addColumn(new Column("boolean_col", TypeInfoFactory.BOOLEAN));
    allTypeSchema.addColumn(new Column("datetime_col", TypeInfoFactory.DATETIME));
    allTypeSchema.addColumn(new Column("string_col", TypeInfoFactory.STRING));
    allTypeSchema.addColumn(new Column("decimal_col", TypeInfoFactory.DECIMAL));
    allTypeSchema.addColumn(new Column("map_col", TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.BIGINT)));
    allTypeSchema.addColumn(new Column("array_col", TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING)));
    allTypeSchema.addColumn(new Column("tinyint_col", TypeInfoFactory.TINYINT));
    allTypeSchema.addColumn(new Column("smallint_col", TypeInfoFactory.SMALLINT));
    allTypeSchema.addColumn(new Column("int_col", TypeInfoFactory.INT));
    allTypeSchema.addColumn(new Column("float_col", TypeInfoFactory.FLOAT));
    allTypeSchema.addColumn(new Column("char_col", TypeInfoFactory.getCharTypeInfo(10)));
    allTypeSchema.addColumn(new Column("varchar_col", TypeInfoFactory.getVarcharTypeInfo(20)));
    allTypeSchema.addColumn(new Column("date_col", TypeInfoFactory.DATE));
    allTypeSchema.addColumn(new Column("timestamp_col", TypeInfoFactory.TIMESTAMP));
    allTypeSchema.addColumn(new Column("binary_col", TypeInfoFactory.BINARY));
    allTypeSchema.addColumn(new Column("interval_day_time_col", TypeInfoFactory.INTERVAL_DAY_TIME));
    allTypeSchema.addColumn(new Column("interval_year_month_col", TypeInfoFactory.INTERVAL_YEAR_MONTH));
    allTypeSchema.addColumn(new Column("struct_col", TypeInfoFactory.getStructTypeInfo(
        Arrays.asList("field1", "field2"),
        Arrays.asList(TypeInfoFactory.STRING, TypeInfoFactory.BIGINT))));
    allTypeSchema.addColumn(new Column("json_col", TypeInfoFactory.JSON));
    allTypeSchema.addColumn(new Column("timestamp_ntz_col", TypeInfoFactory.TIMESTAMP_NTZ));

    odps.tables().delete(tableName, true);
    odps.tables().newTableCreator(tableName, allTypeSchema)
        .ifNotExists()
        .withLifeCycle(1L)
        .withHints(ImmutableMap.of("odps.sql.type.system.odps2", "true"))
        .withTblProperties(ImmutableMap.of("columnar.nested.type", "true"))
        .create();
  }


  @Test
  public void testParseNull() throws Exception {
    TableTunnel.UploadSession uploadSession = odps.tableTunnel()
        .createUploadSession(odps.getDefaultProject(), tableName);
    RecordWriter recordWriter = uploadSession.openRecordWriter(0L);
    Record record = uploadSession.newRecord();

    recordWriter.write(record);
    recordWriter.close();
    uploadSession.commit(new Long[]{0L});

    Instance instance = SQLTask.run(odps, "select * from " + tableName + ";");
    instance.waitForSuccess();

    Map<String, String> results = instance.getTaskResults();
    String selectResult = results.get(SQLTask.AnonymousSQLTaskName);

    if (selectResult != null) {
      System.out.println(CSVRecordParser.parse(selectResult, allTypeSchema).getRecords());
    }
  }

  @Test
  public void testParseMaxValue() throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    TableTunnel.UploadSession uploadSession = odps.tableTunnel()
        .createUploadSession(odps.getDefaultProject(), tableName);
    RecordWriter recordWriter = uploadSession.openRecordWriter(0L);
    Record record = uploadSession.newRecord();
    record.set(0, Long.MAX_VALUE);
    record.set(1, Double.MAX_VALUE);
    record.set(2, true);
    record.set(3, Instant.ofEpochMilli(25340230799999L).atZone(ZoneId.systemDefault()));
    record.set(4, ",ss,\"''.,");
    record.set(5, BigDecimal.valueOf(1));
    record.set(6, ImmutableMap.of("key", Long.MAX_VALUE));
    record.set(7, Arrays.asList("ss", "ss"));
    record.set(8, (byte) 127);
    record.set(9, (short) 32767);
    record.set(10, Integer.MAX_VALUE);
    record.set(11, Float.MAX_VALUE);
    record.set(12, new Char("ss"));
    record.set(13, new Varchar("ss"));
    record.set(14, LocalDate.parse("9999-12-31", DateTimeFormatter.ISO_DATE));
    record.set(15,  Instant.ofEpochMilli(25340230799999L));
    record.set(16, new Binary(",ss,;;''".getBytes(StandardCharsets.UTF_8)));
//    record.set(17, new IntervalDayTime(Long.MAX_VALUE, Integer.MAX_VALUE));
//    record.set(18, new IntervalYearMonth(Integer.MAX_VALUE));
    record.set(19, new SimpleStruct((StructTypeInfo) allTypeSchema.getColumn(19).getTypeInfo(), ImmutableList.of("ss", Long.MAX_VALUE)));
    record.set(20, "{\"hello\":\"world\"}");
    record.set(21, LocalDateTime.now());

    recordWriter.write(record);
    recordWriter.close();
    uploadSession.commit(new Long[]{0L});

    Instance instance = SQLTask.run(odps, "select * from " + tableName + ";");
    instance.waitForSuccess();

    Map<String, String> results = instance.getTaskResults();
    String selectResult = results.get(SQLTask.AnonymousSQLTaskName);

    if (selectResult != null) {
      System.out.println(CSVRecordParser.parse(selectResult, allTypeSchema).getRecords());
    }
  }

}
