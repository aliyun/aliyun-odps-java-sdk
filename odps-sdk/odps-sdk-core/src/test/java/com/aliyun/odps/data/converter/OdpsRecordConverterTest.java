package com.aliyun.odps.data.converter;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.*;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.type.*;
import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.*;


public class OdpsRecordConverterTest {

    private static final OdpsRecordConverter defaultConverter = OdpsRecordConverter.defaultConverter();
    private static StructTypeInfo structTypeInfo;
    private static SimpleStruct simpleStruct;

    @BeforeClass
    public static void setup() {
        structTypeInfo = TypeInfoFactory.getStructTypeInfo(
                Arrays.asList("f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21".split(",")),
                Arrays.asList(
                        TypeInfoFactory.TINYINT,
                        TypeInfoFactory.SMALLINT,
                        TypeInfoFactory.INT,
                        TypeInfoFactory.BIGINT,

                        TypeInfoFactory.getCharTypeInfo(5),
                        TypeInfoFactory.getVarcharTypeInfo(10),
                        TypeInfoFactory.STRING,

                        TypeInfoFactory.FLOAT,
                        TypeInfoFactory.DOUBLE,
                        TypeInfoFactory.DECIMAL,

                        TypeInfoFactory.BOOLEAN,
                        TypeInfoFactory.BINARY,

                        TypeInfoFactory.DATE,
                        TypeInfoFactory.DATETIME,
                        TypeInfoFactory.TIMESTAMP,
                        TypeInfoFactory.TIMESTAMP_NTZ,

                        TypeInfoParser.getTypeInfoFromTypeString("array<int>"),
                        TypeInfoParser.getTypeInfoFromTypeString("array<map<int, string>>"),
                        TypeInfoParser.getTypeInfoFromTypeString("array<map<int, string>>"),
                        TypeInfoParser.getTypeInfoFromTypeString("map<int, array<datetime>>"),
                        TypeInfoParser.getTypeInfoFromTypeString("struct<f1:string, f2: array<date>, f3: map<int, timestamp>>")
                )
        );

        List<Map<Integer, String>> arrayWithMap = new ArrayList();
        Map<Integer, String> m1 = new HashMap<>();
        m1.put(1, "str1");
        m1.put(2, "str2");
        Map<Integer, String> m2 = new HashMap<>();
        m2.put(1, "str3");
        m2.put(2, "str4");
        arrayWithMap.add(m1);
        arrayWithMap.add(m2);

        Map<Integer, List<ZonedDateTime>> map = new HashMap<>();
        map.put(1, Arrays.asList(ZonedDateTime.of(1990, 1, 1,
                1, 1, 1, 123456789,
                ZoneId.systemDefault())));

        Map<Integer, Instant> mapInStruct = new HashMap<>();
        SimpleStruct s = new SimpleStruct(
                (StructTypeInfo) TypeInfoParser.getTypeInfoFromTypeString("struct<f1:string, f2:array<date>, f3:map<int, timestamp>>"),
                Arrays.asList("a",
                        Arrays.asList(LocalDate.of(1922, 1, 1),
                                LocalDate.of(2022, 2, 2)),
                        mapInStruct)
        );


        List<Object> values = Arrays.asList(
                Byte.parseByte("1"),
                Short.parseShort("2"),
                Integer.parseInt("3"),
                Long.parseLong("4"),
                new Char("char"),
                new Varchar("varchar"),
                "string",
                1.0F,
                2.0D,
                BigDecimal.valueOf(3.0),
                Boolean.TRUE,
                new Binary("hello".getBytes(StandardCharsets.UTF_8)),
                LocalDate.of(2023, 9, 5),
                ZonedDateTime.of(2023, 9, 5,
                        10, 9, 8, 123456789,
                        ZoneId.systemDefault()),
                ZonedDateTime.of(2023, 9, 5,
                        10, 9, 8, 123456789,
                        ZoneId.systemDefault()).toInstant(),
                LocalDateTime.of(2023, 9, 5,
                        10, 9, 8, 123456789),
                Arrays.asList(1, 2, 3),
                arrayWithMap,
                Collections.EMPTY_LIST,
                map,
                s
        );

        simpleStruct = new SimpleStruct(structTypeInfo, values);

    }

    @Test
    public void testFormatBoolean() {

        String trueStr = "true";
        String falseStr = "false";
        String falseUpperStr = "FALSE";

        TypeInfo boolType = TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN);

        Assert.assertEquals(trueStr, defaultConverter.formatObject(true, boolType));
        Assert.assertEquals(falseStr, defaultConverter.formatObject(false, boolType));


        Assert.assertEquals(true, defaultConverter.parseObject(trueStr, boolType));
        Assert.assertEquals(false, defaultConverter.parseObject(falseStr, boolType));
        Assert.assertEquals(false, defaultConverter.parseObject(falseUpperStr, boolType));

    }

    @Test
    public void testFormatParseInteger() {
        // 1. test tinyint/smallint/int/bigint min/max val
        // 2. test exception(long.min)

        TypeInfo tinyint = TypeInfoFactory.TINYINT;
        TypeInfo smallint = TypeInfoFactory.SMALLINT;
        TypeInfo intType = TypeInfoFactory.INT;
        TypeInfo bigint = TypeInfoFactory.BIGINT;

        Assert.assertEquals("127", defaultConverter.formatObject(Byte.MAX_VALUE, tinyint));
        Assert.assertEquals("-128", defaultConverter.formatObject(Byte.MIN_VALUE, tinyint));
        Assert.assertEquals(Byte.MAX_VALUE, defaultConverter.parseObject("127", tinyint));
        Assert.assertEquals(Byte.MIN_VALUE, defaultConverter.parseObject("-128", tinyint));

        Assert.assertEquals("32767", defaultConverter.formatObject(Short.MAX_VALUE, smallint));
        Assert.assertEquals("-32768", defaultConverter.formatObject(Short.MIN_VALUE, smallint));
        Assert.assertEquals(Short.MAX_VALUE, defaultConverter.parseObject("32767", smallint));
        Assert.assertEquals(Short.MIN_VALUE, defaultConverter.parseObject("-32768", smallint));

        Assert.assertEquals("2147483647", defaultConverter.formatObject(Integer.MAX_VALUE, intType));
        Assert.assertEquals("-2147483648", defaultConverter.formatObject(Integer.MIN_VALUE, intType));
        Assert.assertEquals(Integer.MAX_VALUE, defaultConverter.parseObject("2147483647", intType));
        Assert.assertEquals(Integer.MIN_VALUE, defaultConverter.parseObject("-2147483648", intType));

        // MC bigint min val = Long.MIN_VALUE+1
        Assert.assertEquals("9223372036854775807", defaultConverter.formatObject(Long.MAX_VALUE, bigint));
        Assert.assertEquals("-9223372036854775807", defaultConverter.formatObject(Long.MIN_VALUE + 1, bigint));
        Assert.assertEquals(Long.MAX_VALUE, defaultConverter.parseObject("9223372036854775807", bigint));
        Assert.assertEquals(Long.MIN_VALUE + 1, defaultConverter.parseObject("-9223372036854775807", bigint));

        testError(() -> {
            defaultConverter.parseObject("128", tinyint);
        });

        testError(() -> {
            defaultConverter.parseObject("-9223372036854775808", tinyint);
        });


    }

    @Test
    public void testFormatFloat() {
        TypeInfo typeInfo = TypeInfoFactory.FLOAT;

        Object[] cases = {
                Float.NaN, "NaN", "NaN",
                Float.NaN, "nan", "NaN",
                Float.NaN, "+nan", "NaN",
                Float.POSITIVE_INFINITY, "Infinity", "Infinity",
                Float.NEGATIVE_INFINITY, "-infinity", "-Infinity",
                123.123f, "123.123", "123.123",
                123.123f, "123.123f", "123.123",
                123.0f, "123.0", "123.0",
                123f, "123", "123.0"
        };

        for (int i = 0; i < cases.length; i += 3) {
            System.out.println("float " + i + " " + cases[i]);

            float val = (float) cases[i];
            String inputFormat = (String) cases[i + 1];
            String outputFormat = (String) cases[i + 2];

            Assert.assertEquals(outputFormat, defaultConverter.formatObject(val, typeInfo));
            Assert.assertEquals(val, defaultConverter.parseObject(inputFormat, typeInfo));
        }

    }


    @Test
    public void testFormatDouble() {
        TypeInfo typeInfo = TypeInfoFactory.DOUBLE;

        class TestDoubleItem {

            final double value;
            final String expFormat;
            final String flatFormat;

            public TestDoubleItem(double value, String expFormat, String flatFormat) {
                this.value = value;
                this.expFormat = expFormat;
                this.flatFormat = flatFormat;
            }
        }

        List<TestDoubleItem> testItems = Arrays.asList(
                new TestDoubleItem(Double.NaN, "NaN", "NaN"),
                new TestDoubleItem(Double.POSITIVE_INFINITY, "Infinity", "Infinity"),
                new TestDoubleItem(Double.NEGATIVE_INFINITY, "-Infinity", "-Infinity"),
                new TestDoubleItem(1234567.1234567, "1234567.1234567", "1234567.1234567"),
                new TestDoubleItem(1.2345E2, "123.45", "123.45"),
                new TestDoubleItem(1.2345678901234567, "1.2345678901234567", "1.2345678901234567"),
                // 超过 7 位默认 exp 展示
                new TestDoubleItem(12345678.1234567, "1.23456781234567E7", "1.23456781234567E7"),
                new TestDoubleItem(1.2345E10, "1.2345E10", "1.2345E10"),
                new TestDoubleItem(1.0, "1.0", "1.0"),
                new TestDoubleItem(1, "1.0", "1.0"),
                new TestDoubleItem(0.000000000000001000010000100001, "1.000010000100001E-15",
                        "1.000010000100001E-15")
        );

        for (TestDoubleItem item : testItems) {
            Assert.assertEquals(item.flatFormat, defaultConverter.formatObject(item.value, typeInfo));
            Assert.assertEquals(item.value, defaultConverter.parseObject(item.expFormat, typeInfo));
            Assert.assertEquals(item.value, defaultConverter.parseObject(item.flatFormat, typeInfo));
        }

        Assert.assertEquals(Double.NaN, defaultConverter.parseObject("-nan", typeInfo));
    }


    @Test
    public void testFormatFloatInSqlCompatibleMode() {
        TypeInfo typeInfo = TypeInfoFactory.FLOAT;

        Object[] objects = {
                Float.NaN, "NaN", "NaN",
                Float.NaN, "nan", "NaN",
                Float.POSITIVE_INFINITY, "infinity", "Infinity",
                Float.POSITIVE_INFINITY, "+infinity", "Infinity",
                Float.POSITIVE_INFINITY, "+inf", "Infinity",
                Float.NEGATIVE_INFINITY, "-inf", "-Infinity",
                Float.NEGATIVE_INFINITY, "-infinity", "-Infinity",
                Float.MIN_VALUE, "1.4e-45", "1.401298e-45",
                Float.MAX_VALUE, "3.4028235e38", "3.402823e+38",

                // scientific notation
                0.00012345f, "0.00012345", "0.00012345",
                -0.00012345f, "-0.00012345", "-0.00012345",
                0.000012345f, "0.000012345", "1.2345e-05",
                -0.000012345f, "-0.000012345", "-1.2345e-05",
                1234567f, "1234567", "1234567",
                -1234567f, "-1234567", "-1234567",
                12345678f, "12345678", "1.234568e+07",
                -12345678f, "-12345678", "-1.234568e+07",

                // round even
                1.333333f, "1.333333", "1.333333",
                1.2345678f, "1.2345678", "1.234568",
                16777115f, "16777115", "1.677712e+07",
                16777125f, "16777125", "1.677712e+07"
        };

        OdpsRecordConverter converter = OdpsRecordConverter.builder().floatingNumberFormatCompatible().build();
        for (int i = 0; i < objects.length; i += 3) {
            float val = (float) objects[i];
            String inputFormat = (String) objects[i + 1];
            String outputFormat = (String) objects[i + 2];


            System.out.println(val);
            Assert.assertEquals(val, converter.parseObject(inputFormat, typeInfo));
            Assert.assertEquals(outputFormat, converter.formatObject(val, typeInfo));
        }
    }

    @Test
    public void testFormatDoubleInSqlCompatibleMode() {
        TypeInfo typeInfo = TypeInfoFactory.DOUBLE;

        Object[][] objects = {
                {Double.NaN, "NaN", "NaN"},
                {Double.NaN, "nan", "NaN"},
                {Double.POSITIVE_INFINITY, "infinity", "Infinity"},
                {Double.POSITIVE_INFINITY, "+infinity", "Infinity"},
                {Double.POSITIVE_INFINITY, "+inf", "Infinity"},
                {Double.NEGATIVE_INFINITY, "-inf", "-Infinity"},
                {Double.NEGATIVE_INFINITY, "-infinity", "-Infinity"},
                //TODO fix this
//                {Double.MIN_VALUE, "4.9e-324", "5e-324"},
                {Double.MIN_VALUE, "4.9e-324", "4.9e-324"},
                {Double.MAX_VALUE, "1.7976931348623157e+308", "1.7976931348623157e308"},

//                // scientific notation
                {0.0000000000000001, "1e-16", "1e-16"},
                {0.00000000000000000001, "1e-20", "1e-20"},
                {0.0000000001, "1e-10", "1e-10"},
                {-0.001, "-0.001", "-0.001"},
                {0.001, "0.001", "0.001"},
                {-0.1, "-0.1", "-0.1"},
                {0.1, "0.1", "0.1"},
                {-1.0, "-1", "-1.0"},
                {1.0, "1", "1.0"},
                {-100.0, "-100", "-100.0"},
                {100.0, "100", "100.0"},
                {100000000000000000000000000.0, "1e26", "1e26"},
                {100.001, "100.001", "100.001"},
                {0.0000012345, "0.0000012345", "0.0000012345"},
                {-0.0000012345, "-0.0000012345", "-0.0000012345"},
                {0.00000012345, "0.00000012345", "1.2345e-07"},
                {-0.00000012345, "-0.00000012345", "-1.2345e-07"},
                {1234567890123456789.0, "1234567890123456789.0", "1234567890123456800.0"},
                {12345678901234567890.0, "12345678901234567890.0", "12345678901234567000.0"},
                {-123456789012345678901.0, "-123456789012345678901.0", "-1.2345678901234568e20"},
                {123456789012345678901.0, "123456789012345678901.0", "1.2345678901234568e20"},
        };

        OdpsRecordConverter converter = OdpsRecordConverter.builder().floatingNumberFormatCompatible().build();
        for (Object[] object : objects) {
            double val = (double) object[0];
            String inputFormat = (String) object[1];
            String outputFormat = (String) object[2];
            System.out.println(outputFormat);

            Assert.assertEquals(val, converter.parseObject(inputFormat, typeInfo));
            Assert.assertEquals(outputFormat, converter.formatObject(val, typeInfo));
        }
    }

    @Test
    public void testFormatDecimal() {
        // 1. test parse rounding
        // 2. test integer only
        // 3. test max/min
        // 4. test parse exception
        // 5. test zero_fill formatObject

        DecimalTypeInfo[] typeInfos = {
                TypeInfoFactory.getDecimalTypeInfo(2, 1),
                TypeInfoFactory.getDecimalTypeInfo(2, 1),
                TypeInfoFactory.getDecimalTypeInfo(2, 1),
                TypeInfoFactory.getDecimalTypeInfo(2, 1),
                TypeInfoFactory.getDecimalTypeInfo(2, 1),
                TypeInfoFactory.getDecimalTypeInfo(38, 0),
                TypeInfoFactory.getDecimalTypeInfo(38, 0),
                TypeInfoFactory.getDecimalTypeInfo(18, 18),
                TypeInfoFactory.getDecimalTypeInfo(18, 18),
                TypeInfoFactory.getDecimalTypeInfo(38, 18),
                TypeInfoFactory.getDecimalTypeInfo(38, 18),
                TypeInfoFactory.getDecimalTypeInfo(38, 18)
        };

        BigDecimal[] bigDecimals = {
                new BigDecimal("1.1"),
                new BigDecimal("1.1"),
                new BigDecimal("1.2"),
                new BigDecimal("1"),
                new BigDecimal("1"),
                new BigDecimal("12345678901234567890123456789012345678"),
                new BigDecimal("99999999999999999999999999999999999999"),
                new BigDecimal("0.123456789012345678"),
                new BigDecimal("0.000000000000000001"),
                new BigDecimal("99999999999999999999.999999999999999999"),
                new BigDecimal("10000000000000000000.000000000000000001"),
                new BigDecimal("1.1")
        };

        String[] outputFormat = {
                "1.1",
                "1.1",
                "1.2",
                "1",
                "1",
                "12345678901234567890123456789012345678",
                "99999999999999999999999999999999999999",
                "0.123456789012345678",
                "0.000000000000000001",
                "99999999999999999999.999999999999999999",
                "10000000000000000000.000000000000000001",
                "1.1"
        };

        String[] customOutputFormat = {
                "1.1",
                "1.1",
                "1.2",
                "1.0",
                "1.0",
                "12345678901234567890123456789012345678",
                "99999999999999999999999999999999999999",
                "0.123456789012345678",
                "0.000000000000000001",
                "99999999999999999999.999999999999999999",
                "10000000000000000000.000000000000000001",
                "1.100000000000000000"
        };

        String[] inputFormat = new String[]{
                "1.11", // test rounding
                "1.15", // test rounding
                "1.16", // test rounding
                "1",
                "1.0",
                "12345678901234567890123456789012345678",
                "99999999999999999999999999999999999999",
                "0.123456789012345678",
                "0.000000000000000001",
                "99999999999999999999.999999999999999999",
                "10000000000000000000.000000000000000001",
                "1.1"
        };

        OdpsRecordConverter customFormatter = OdpsRecordConverter.builder().decimalFormatWithZeroPadding().build();

        for (int i = 0; i < bigDecimals.length; i++) {
            bigDecimals[i] = bigDecimals[i].setScale(typeInfos[i].getScale());
            System.out.println("decimal" + i);
            Assert.assertEquals(outputFormat[i], defaultConverter.formatObject(bigDecimals[i], typeInfos[i]));
            Assert.assertEquals(customOutputFormat[i], customFormatter.formatObject(bigDecimals[i], typeInfos[i]));
            Assert.assertEquals(bigDecimals[i], defaultConverter.parseObject(inputFormat[i], typeInfos[i]));
        }

        try {
            defaultConverter.parseObject("123.123", TypeInfoFactory.getDecimalTypeInfo(5, 3));
            assert false;
        } catch (IllegalArgumentException ignore) {

        }

    }

    @Test
    public void temp() throws TunnelException, IOException, ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Date date = format.parse("20221010101010");
        System.out.println(date);
    }

    @Test
    public void testFormatDate() {
        LocalDate[] date = {
                LocalDate.of(0, 1, 1),
                LocalDate.of(0, 1, 1),
                LocalDate.of(1, 1, 1),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(2022, 1, 1),
                LocalDate.of(9999, 12, 31)
        };

        String[] defaultInputFormat = {
                "0000-01-01",
                "0000-1-1",
                "0001-01-01",
                "1900-01-01",
                "2022-01-01",
                "9999-12-31"
        };

        String[] defaultOutputFormat = {
                "0000-01-01",
                "0000-01-01",
                "0001-01-01",
                "1900-01-01",
                "2022-01-01",
                "9999-12-31"
        };

        String[] customFormat = {
                "0000 01 01",
                "0000 01 01",
                "0001 01 01",
                "1900 01 01",
                "2022 01 01",
                "9999 12 31"
        };

        TypeInfo dateType = TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE);
        OdpsRecordConverter converter = OdpsRecordConverter.builder().dateFormat("uuuu MM dd").build();

        for (int i = 0; i < date.length; i++) {
            System.out.println(i);
            Assert.assertEquals(defaultOutputFormat[i], defaultConverter.formatObject(date[i], dateType));
            Assert.assertEquals(date[i], defaultConverter.parseObject(defaultInputFormat[i], dateType));
            Assert.assertEquals(customFormat[i], converter.formatObject(date[i], dateType));
            Assert.assertEquals(date[i], converter.parseObject(customFormat[i], dateType));
        }

        String wrongFormat = "DATE illegal";
        try {
            converter = OdpsRecordConverter.builder().dateFormat(wrongFormat).build();
            assert false;
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(wrongFormat));
        }

    }


    @Test
    public void testFormatDatetime() {
        ZonedDateTime[] zdt = {
                ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()),
                ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()),
                ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()),
                ZonedDateTime.of(1900, 1, 1, 8, 0, 0, 123000000, ZoneId.systemDefault()),
        };

        String[] errInputFormat = {
                "0-1-1 0:0:0.000"
        };

        String[] defaultInputFormat = {
                "0000-1-1 0:0:0.000",
                "0000-01-01 0:0:0.000",
                "0000-01-01 0:0:0.000",
                "1900-01-01 08:00:00.123456789"
        };

        String[] defaultOutputFormat = {
                "0000-01-01 00:00:00",
                "0000-01-01 00:00:00",
                "0000-01-01 00:00:00",
                "1900-01-01 08:00:00"
        };

        String[] customFormat = {
                "00000101 00:00:00.000",
                "00000101 00:00:00.000",
                "00000101 00:00:00.000",
                "19000101 08:00:00.123"
        };

        TypeInfo datetime = TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATETIME);
        OdpsRecordConverter converter = OdpsRecordConverter.builder().datetimeFormat("uuuuMMdd HH:mm:ss.SSS").build();

        for (int i = 0; i < zdt.length; i++) {
            System.out.println(i);
            Assert.assertEquals(defaultOutputFormat[i], defaultConverter.formatObject(zdt[i], datetime));
            Assert.assertEquals(zdt[i], defaultConverter.parseObject(defaultInputFormat[i], datetime));
            Assert.assertEquals(customFormat[i], converter.formatObject(zdt[i], datetime));
            Assert.assertEquals(zdt[i], converter.parseObject(customFormat[i], datetime));
        }

    }

    @Test
    public void testFormatTimestamp() {

        Instant[] instants = {
                ZonedDateTime.of(0, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()).toInstant(),
                ZonedDateTime.of(0, 1, 1, 0, 0, 0, 123000000, ZoneId.systemDefault()).toInstant(),
                ZonedDateTime.of(0, 1, 1, 0, 0, 0, 123456789, ZoneId.systemDefault()).toInstant(),
                ZonedDateTime.of(1900, 1, 1, 8, 0, 0, 123456789, ZoneId.systemDefault()).toInstant(),
        };

        String[] defaultInputFormat = {
                "0000-1-1 0:0:0",
                "0000-01-01 0:0:0.123",
                "0000-01-01 0:0:0.123456789",
                "1900-01-01 08:00:00.123456789"
        };

        String[] defaultOutputFormat = {
                "0000-01-01 00:00:00",
                "0000-01-01 00:00:00.123",
                "0000-01-01 00:00:00.123456789",
                "1900-01-01 08:00:00.123456789"
        };

        String[] customFormat = {
                "00000101 00:00:00.000000000",
                "00000101 00:00:00.123000000",
                "00000101 00:00:00.123456789",
                "19000101 08:00:00.123456789"
        };

        TypeInfo datetime = TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP);
        OdpsRecordConverter converter = OdpsRecordConverter.builder()
                .timestampFormat("uuuuMMdd HH:mm:ss.SSSSSSSSS").build();

        for (int i = 0; i < instants.length; i++) {
            System.out.println(i);
            Assert.assertEquals(defaultOutputFormat[i], defaultConverter.formatObject(instants[i], datetime));
            Assert.assertEquals(instants[i], defaultConverter.parseObject(defaultInputFormat[i], datetime));
            Assert.assertEquals(customFormat[i], converter.formatObject(instants[i], datetime));
            Assert.assertEquals(instants[i], converter.parseObject(customFormat[i], datetime));
        }

    }

    @Test
    public void testTimestampNtz() {
        LocalDateTime[] times = {
                LocalDateTime.of(0, 1, 1, 0, 0, 0, 0),
                LocalDateTime.of(0, 1, 1, 0, 0, 0, 123000000),
                LocalDateTime.of(0, 1, 1, 0, 0, 0, 123123123),
                LocalDateTime.of(1900, 1, 1, 8, 0, 0, 123123123),
                LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999999)
        };

        String[] defaultInputFormat = {
                "0000-1-1 0:0:0",
                "0000-01-01 0:0:0.123",
                "0000-01-01 0:0:0.123123123",
                "1900-01-01 08:00:00.123123123",
                "9999-12-31 23:59:59.999999999"
        };

        String[] defaultOutputFormat = {
                "0000-01-01 00:00:00",
                "0000-01-01 00:00:00.123",
                "0000-01-01 00:00:00.123123123",
                "1900-01-01 08:00:00.123123123",
                "9999-12-31 23:59:59.999999999"
        };

        String[] customFormat = {
                "00000101 00:00:00.000000000",
                "00000101 00:00:00.123000000",
                "00000101 00:00:00.123123123",
                "19000101 08:00:00.123123123",
                "99991231 23:59:59.999999999"
        };

        TypeInfo datetime = TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP_NTZ);
        OdpsRecordConverter formatter = OdpsRecordConverter.builder()
                .timestampNtzFormat("uuuuMMdd HH:mm:ss.SSSSSSSSS")
                .timezone("Asia/Shanghai")
                .build();
        OdpsRecordConverter parser = OdpsRecordConverter.builder()
                .timestampNtzFormat("uuuuMMdd HH:mm:ss.SSSSSSSSS")
                .timezone("America/New_York")
                .build();

        for (int i = 0; i < times.length; i++) {
            System.out.println(i);
            Assert.assertEquals(defaultOutputFormat[i], defaultConverter.formatObject(times[i], datetime));
            Assert.assertEquals(times[i], defaultConverter.parseObject(defaultInputFormat[i], datetime));
            Assert.assertEquals(customFormat[i], formatter.formatObject(times[i], datetime));
            Assert.assertEquals(times[i], parser.parseObject(customFormat[i], datetime));
        }
    }

    @Test
    public void testFormatBinary() {
        OdpsRecordConverter base64Converter = OdpsRecordConverter.builder().binaryFormatBase64().build();


        Binary[] binaries = {
                new Binary("utf8binary".getBytes(StandardCharsets.UTF_8)),
                new Binary(Base64.getDecoder().decode("base64binary")),
        };


        System.out.println(defaultConverter.formatObject(binaries[0], TypeInfoFactory.BINARY));
        System.out.println(base64Converter.formatObject(binaries[1], TypeInfoFactory.BINARY));
    }

    @Test
    public void testFormatJson() {
        String jsonStr = "{\"key\":\"val\"}";
        JsonValue jsonValue = new SimpleJsonValue(jsonStr);
        Assert.assertEquals(jsonStr, defaultConverter.formatObject(jsonValue, TypeInfoFactory.JSON));

        JsonValue parsedJson = (JsonValue) defaultConverter.parseObject(jsonStr, TypeInfoFactory.JSON);
        Assert.assertEquals("val", parsedJson.get("key").getAsString());
    }


    /**
     * test complex data type
     * 1. complex type with simple type
     * 2. complex type with use custom format function
     * 3. nest complex type
     */

    @Test
    public void testFormatArray() {
        List<Integer> list = Arrays.asList(1, 2, 3);
        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString("Array<int>");

        OdpsRecordConverter converter = OdpsRecordConverter.builder().build();
        Assert.assertEquals("[1,2,3]", converter.formatObject(list, typeInfo));

        converter = OdpsRecordConverter.builder().complexFormatJsonStr().build();
        Assert.assertEquals("[\"1\",\"2\",\"3\"]", converter.formatObject(list, typeInfo));

        converter = OdpsRecordConverter.builder().complexFormatHumanReadable().build();
        Assert.assertEquals("[1, 2, 3]", converter.formatObject(list, typeInfo));

    }

    @Test
    public void testFormatMap() {
        Map<Integer, Double> map = new HashMap<>();
        map.put(1, 1.1);
        map.put(2, 2.2);

        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString("Map<int, double>");

        OdpsRecordConverter converter = OdpsRecordConverter.builder().build();
        Assert.assertEquals("{\"1\":1.1,\"2\":2.2}", converter.formatObject(map, typeInfo));

        converter = OdpsRecordConverter.builder().complexFormatJsonStr().build();
        Assert.assertEquals("{\"1\":\"1.1\",\"2\":\"2.2\"}", converter.formatObject(map, typeInfo));

        converter = OdpsRecordConverter.builder().complexFormatHumanReadable().build();
        Assert.assertEquals("{1:1.1, 2:2.2}", converter.formatObject(map, typeInfo));

    }

    @Test
    public void testComplexDataInJson() {

        String expectedJson = "{\n"
                + "    \"f1\": 1, \n"
                + "    \"f2\": 2, \n"
                + "    \"f3\": 3, \n"
                + "    \"f4\": 4, \n"
                + "    \"f5\": \"char\", \n"
                + "    \"f6\": \"varchar\", \n"
                + "    \"f7\": \"string\", \n"
                + "    \"f8\": 1.0, \n"
                + "    \"f9\": 2.0, \n"
                + "    \"f10\": 3, \n"
                + "    \"f11\": true, \n"
                + "    \"f12\": \"hello\", \n"
                + "    \"f13\": \"2023-09-05\", \n"
                + "    \"f14\": \"2023-09-05 10:09:08\", \n"
                + "    \"f15\": \"2023-09-05 10:09:08.123456789\", \n"
                + "    \"f16\": \"2023-09-05 10:09:08.123456789\", \n"
                + "    \"f17\": [\n"
                + "        1, \n"
                + "        2, \n"
                + "        3\n"
                + "    ], \n"
                + "    \"f18\": [\n"
                + "        {\n"
                + "            \"1\": \"str1\", \n"
                + "            \"2\": \"str2\"\n"
                + "        }, \n"
                + "        {\n"
                + "            \"1\": \"str3\", \n"
                + "            \"2\": \"str4\"\n"
                + "        }\n"
                + "    ], \n"
                + "    \"f19\": [ ], \n"
                + "    \"f20\": {\n"
                + "        \"1\": [\n"
                + "            \"1990-01-01 01:01:01\"\n"
                + "        ]\n"
                + "    }, \n"
                + "    \"f21\": {\n"
                + "        \"f1\": \"a\", \n"
                + "        \"f2\": [\n"
                + "            \"1922-01-01\", \n"
                + "            \"2022-02-02\"\n"
                + "        ], \n"
                + "        \"f3\": { }\n"
                + "    }\n"
                + "}";

        String formattedJson = defaultConverter.formatObject(simpleStruct, structTypeInfo);
        System.out.println(formattedJson);

        Assert.assertEquals(new Gson().toJson(JsonParser.parseString(expectedJson)), formattedJson);
    }

    @Test
    public void testComplexDataInJsonString() {

        String expectedJson = "{\n"
                + "    \"f1\": \"1\", \n"
                + "    \"f2\": \"2\", \n"
                + "    \"f3\": \"3\", \n"
                + "    \"f4\": \"4\", \n"
                + "    \"f5\": \"char\", \n"
                + "    \"f6\": \"varchar\", \n"
                + "    \"f7\": \"string\", \n"
                + "    \"f8\": \"1.0\", \n"
                + "    \"f9\": \"2.0\", \n"
                + "    \"f10\": \"3\", \n"
                + "    \"f11\": \"true\", \n"
                + "    \"f12\": \"hello\", \n"
                + "    \"f13\": \"2023-09-05\", \n"
                + "    \"f14\": \"2023-09-05 10:09:08\", \n"
                + "    \"f15\": \"2023-09-05 10:09:08.123456789\", \n"
                + "    \"f16\": \"2023-09-05 10:09:08.123456789\", \n"
                + "    \"f17\": [\n"
                + "        \"1\", \n"
                + "        \"2\", \n"
                + "        \"3\"\n"
                + "    ], \n"
                + "    \"f18\": [\n"
                + "        {\n"
                + "            \"1\": \"str1\", \n"
                + "            \"2\": \"str2\"\n"
                + "        }, \n"
                + "        {\n"
                + "            \"1\": \"str3\", \n"
                + "            \"2\": \"str4\"\n"
                + "        }\n"
                + "    ], \n"
                + "    \"f19\": [ ], \n"
                + "    \"f20\": {\n"
                + "        \"1\": [\n"
                + "            \"1990-01-01 01:01:01\"\n"
                + "        ]\n"
                + "    }, \n"
                + "    \"f21\": {\n"
                + "        \"f1\": \"a\", \n"
                + "        \"f2\": [\n"
                + "            \"1922-01-01\", \n"
                + "            \"2022-02-02\"\n"
                + "        ], \n"
                + "        \"f3\": { }\n"
                + "    }\n"
                + "}";

        String formattedJson = OdpsRecordConverter.builder().complexFormatJsonStr().build()
                .formatObject(simpleStruct, structTypeInfo);
        System.out.println(formattedJson);

        Assert.assertEquals(new Gson().toJson(JsonParser.parseString(expectedJson)), formattedJson);
    }

    @Test
    public void testComplexDataInObject() {
        String expectedResult = "{f1:1, f2:2, f3:3, f4:4, f5:char, f6:varchar, f7:string, f8:1.0, f9:2.0, f10:3, f11:true, f12:hello, f13:2023-09-05, f14:2023-09-05 10:09:08, f15:2023-09-05 10:09:08.123456789, f16:2023-09-05 10:09:08.123456789, f17:[1, 2, 3], f18:[{1:str1, 2:str2}, {1:str3, 2:str4}], f19:[], f20:{1:[1990-01-01 01:01:01]}, f21:{f1:a, f2:[1922-01-01, 2022-02-02], f3:{}}}";

        String formatResult = OdpsRecordConverter.builder().complexFormatHumanReadable().build()
                .formatObject(simpleStruct, structTypeInfo);
        Assert.assertEquals(expectedResult, formatResult);
    }

    @Test
    public void testCustomOneComplexData() {
        OdpsObjectConverter customArrayConverter = new OdpsObjectConverter() {
            @Override
            public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
                return typeInfo.toString() + ": " + object.toString();
            }

            @Override
            public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
                return null;
            }
        };

        OdpsRecordConverter converter = OdpsRecordConverter.builder()
                .register(OdpsType.ARRAY, customArrayConverter).build();

        Assert.assertEquals("{\"f1\":1,\"f2\":2,\"f3\":3,\"f4\":4,\"f5\":\"char\",\"f6\":\"varchar\",\"f7\":\"string\",\"f8\":1.0,\"f9\":2.0,\"f10\":3,\"f11\":true,\"f12\":\"hello\",\"f13\":\"2023-09-05\",\"f14\":\"2023-09-05 10:09:08\",\"f15\":\"2023-09-05 10:09:08.123456789\",\"f16\":\"2023-09-05 10:09:08.123456789\",\"f17\":\"ARRAY<INT>: [1, 2, 3]\",\"f18\":\"ARRAY<MAP<INT,STRING>>: [{1=str1, 2=str2}, {1=str3, 2=str4}]\",\"f19\":\"ARRAY<MAP<INT,STRING>>: []\",\"f20\":{\"1\":\"ARRAY<DATETIME>: [1990-01-01T01:01:01.123456789+08:00[Asia/Shanghai]]\"},\"f21\":{\"f1\":\"a\",\"f2\":\"ARRAY<DATE>: [1922-01-01, 2022-02-02]\",\"f3\":{}}}",
                converter.formatObject(simpleStruct, structTypeInfo));
    }

    @Test
    public void testCustomFormat() {
        OdpsObjectConverter intConverter = new OdpsObjectConverter() {
            @Override
            public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
                DecimalFormat format = new DecimalFormat();
                format.setGroupingUsed(true);
                return format.format(object);
            }

            @Override
            public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
                return null;
            }
        };

        OdpsRecordConverter converter = OdpsRecordConverter.builder()
                .register(OdpsType.INT, intConverter).build();

        String formattedInt = converter.formatObject(100000, TypeInfoFactory.INT);
        Assert.assertEquals("100,000", formattedInt);

        ArrayRecord record = new ArrayRecord(new Column[]{new Column("i", OdpsType.INT)});
        record.set(0, 100000);
        Assert.assertEquals("100,000", converter.formatRecord(record)[0]);
    }

    static class MockRecord extends ArrayRecord {

        public MockRecord(Column[] columns) {
            super(columns);
        }

    }

    @Test
    public void testCustomRecord() {
        String[] s = {"a", "1"};
        Column[] columns = {
                new Column("a", TypeInfoFactory.STRING),
                new Column("b", TypeInfoFactory.INT)
        };


        OdpsRecordConverter converter =
                OdpsRecordConverter.builder().registerRecordProvider(MockRecord::new).build();

        Record r = converter.parseRecord(s, columns);
        assert r instanceof MockRecord;

        r = defaultConverter.parseRecord(s, columns);
        assert r instanceof ArrayRecord;
    }

    @Test
    public void testNull() {
        Assert.assertEquals(ConverterConstant.NULL_OUTPUT_FORMAT_DEFAULT,
                defaultConverter.formatObject(null, TypeInfoFactory.INT));

        Assert.assertEquals("null", OdpsRecordConverter.builder().nullFormat("null").build().
                formatObject(null, TypeInfoFactory.INT));
    }

    @Test
    public void testTypeCheck() {
        try {
            defaultConverter.formatObject(1, TypeInfoFactory.FLOAT);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Cannot format 1(class java.lang.Integer) to ODPS type: FLOAT, expect java class: java.lang.Float", e.getMessage());
        }
    }

    public static void testError(Runnable runnable) {
        try {
            runnable.run();
            assert false;
        } catch (Exception ignore) {
        }
    }
}