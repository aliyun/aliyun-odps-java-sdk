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
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
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
      System.out.println(CSVRecordParser.parse(selectResult, allTypeSchema, "UTC").getRecords());
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
      System.out.println(
        CSVRecordParser.parse(selectResult, allTypeSchema, "Asia/Shanghai").getRecords());
    }
  }

  @Test
  public void testMaxQA() throws OdpsException {
    String
      csv =
      "\"test_case_description\",\"c_tinyint\",\"c_smallint\",\"c_int\",\"c_bigint\",\"c_float\",\"c_double\",\"c_decimal_std\",\"c_decimal_extended\",\"c_decimal_integer\",\"c_varchar\",\"c_char\",\"c_string\",\"c_binary\",\"c_date\",\"c_datetime\",\"c_timestamp\",\"c_timestamp_ntz\",\"c_boolean\"\n"
      + "\"Zero and Empty Values\",0,0,0,0,0,0.0,\"0\",\"0\",\"0\",\"\",\"          \",\"\",\"\",\"1970-01-01\",1970-01-01 00:00:00,\"1970-01-01 00:00:00\",\"1970-01-01 00:00:00\",false\n"
      + "\"All NULL values\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\",\"\\N\"\n"
      + "\"Maximum Values\",127,32767,2147483647,9223372036854775807,3.402823e+38,1.7976931348623157e308,\"99999999999999999999.999999999999999999\",\"\\N\",\"99999999999999999999999999999999999999\",\"varchar_max_len_test\",\"char_max  \",\"A very long string test case with various characters:!@#$%^&*()_+{}[]:;\"\"<>,.?/~` and 中文、日語、한국어\",\"=FF=FE=FD\",\"9999-12-31\",9999-12-31 23:59:59,\"9999-12-31 23:59:59.999999999\",\"9999-12-31 23:59:59.999999999\",true\n"
      + "\"Minimum Values\",-128,-32768,-2147483648,\"\\N\",-3.402823e+38,-1.7976931348623157e308,\"-9999999999999999999.999999999999999999\",\"\\N\",\"-9999999999999999999999999999999999999\",\"varchar_min_test\",\"char_min  \",\"Another string with escapes ' and \"\" and \\\",\"=00=01=02\",\"0001-01-03\",0001-01-01 00:00:00,\"0001-01-01 00:00:00\",\"0000-01-01 00:00:00\",false\n"
      + "\"High and Low Precision\",1,1,1,1,1.401298e-45,5e-324,\"0.000000000000000001\",\"0\",\"1\",\"高精度低精度\",\"高精度       \",\"Test Precision\",\"abc\",\"2023-10-27\",2023-10-27 10:30:15,\"2023-10-27 10:30:15.123456789\",\"2023-10-27 10:30:15.123\",true";

    if (odps.tables().exists("three_pangu2_odps2", "all_types_test_data")) {
      Table table = odps.tables().get("three_pangu2_odps2", "all_types_test_data");
      CSVRecordParser.ParseResult
        parseResult =
        CSVRecordParser.parse(csv, table.getSchema(), "Asia/Shanghai");
      System.out.println(parseResult.getRecords());
    }
  }
}
