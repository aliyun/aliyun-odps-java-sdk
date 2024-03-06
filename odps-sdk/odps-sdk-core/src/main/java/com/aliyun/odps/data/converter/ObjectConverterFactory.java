package com.aliyun.odps.data.converter;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.SimpleJsonValue;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Base64;

import static com.aliyun.odps.data.converter.ConverterConstant.*;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;

class ObjectConverterFactory {

    private enum TinyIntConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return object.toString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return Byte.valueOf(str);
        }

    }

    private enum SmallIntConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return object.toString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return Short.valueOf(str);
        }

    }

    private enum IntConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return object.toString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return Integer.valueOf(str);
        }

    }

    private enum BigIntConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return object.toString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            long longVal = Long.parseLong(str);
            if (Long.MIN_VALUE == longVal) {
                throw new IllegalArgumentException("InvalidData: Bigint out of range.");
            }
            return Long.valueOf(str);
        }

    }

    private enum DoubleConverter implements OdpsObjectConverter {
        TO_STRING {
            @Override
            public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
                double value = (double) object;
                return Double.toString(value);
            }

        },

        SQL_COMPATIBLE {

            private final FloatingNumberFormatter formatter = new FloatingNumberFormatter(17,
                    -6, 20,
                    "#.###################E00", false, true);

            @Override
            public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
                double d = (double) object;
                return formatter.format(d);
            }
        };


        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (isNan(str)) {
                return Double.NaN;
            } else if (isPositiveInfinity(str)) {
                return Double.POSITIVE_INFINITY;
            } else if (isNegativeInfinity(str)) {
                return Double.NEGATIVE_INFINITY;
            }
            return Double.parseDouble(str);
        }
    }

    private enum FloatConverter implements OdpsObjectConverter {

        TO_STRING {
            @Override
            public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
                float f = (float) object;
                return Float.toString(f);
            }

        },

        SQL_COMPATIBLE {

            private final FloatingNumberFormatter formatter = new FloatingNumberFormatter(7, -4, 7,
                    "#.########E00", true, false);

            @Override
            public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
                double f = (double) ((Float) object);
                return formatter.format(f);
            }
        };

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (isNan(str)) {
                return Float.NaN;
            } else if (isPositiveInfinity(str)) {
                return Float.POSITIVE_INFINITY;
            } else if (isNegativeInfinity(str)) {
                return Float.NEGATIVE_INFINITY;
            }
            return Float.parseFloat(str);
        }

    }

    private static class FloatingNumberFormatter {

        MathContext mathContext;
        DecimalFormat posScientificFormat;
        DecimalFormat negScientificFormat;
        int minFractionDigits;
        int maxFractionDigits;
        boolean appendZero;

        private void setExpSymbol(DecimalFormat format, String symbol) {
            DecimalFormatSymbols symbols = format.getDecimalFormatSymbols();
            symbols.setExponentSeparator(symbol);
            format.setDecimalFormatSymbols(symbols);
        }

        FloatingNumberFormatter(int precise, int minFractionDigits, int maxFractionDigits,
                                String scientificFormatPattern, boolean expPlus, boolean appendZero) {
            this.mathContext = new MathContext(precise, RoundingMode.HALF_EVEN);

            this.posScientificFormat = new DecimalFormat(scientificFormatPattern);
            setExpSymbol(this.posScientificFormat, "e");

            this.negScientificFormat = new DecimalFormat(scientificFormatPattern);
            setExpSymbol(this.negScientificFormat, "e");

            if (expPlus) {
                setExpSymbol(this.posScientificFormat, "e+");
            }

            this.minFractionDigits = minFractionDigits;
            this.maxFractionDigits = maxFractionDigits;
            this.appendZero = appendZero;
        }

        private String format(double f) {
            if (!Double.isFinite(f)) {
                // NAN INFINITY
                return Double.toString(f);
            }

            BigDecimal bigDecimal = new BigDecimal(String.valueOf(f));

            // float need round to keep 7 significant digits
            bigDecimal = bigDecimal.round(mathContext);

            // 1.0e01 => 1
            String eStr = String.format("%e", bigDecimal).split("e")[1];
            int e = Integer.parseInt(eStr);

            if (e < minFractionDigits || e >= maxFractionDigits) {
                if (e >= 0) {
                    return posScientificFormat.format(bigDecimal);
                }
                return negScientificFormat.format(bigDecimal);
            } else {
                String result = bigDecimal.stripTrailingZeros().toPlainString();

                // double append zero
                if (appendZero && !result.contains(".") ) {
                    result += ".0";
                }

                return result;
            }

        }
    }

    private enum DecimalConverter implements OdpsObjectConverter {
        PADDING(true),
        NO_PADDING(false);

        private final boolean padding;

        DecimalConverter(boolean padding) {
            this.padding = padding;
        }


        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            BigDecimal decimal = (BigDecimal) object;
            if (padding) {
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
                decimal = decimal.setScale(decimalTypeInfo.getScale(), RoundingMode.HALF_DOWN);
                return decimal.toPlainString();
            }
            // https://stackoverflow.com/questions/18721771/java-removing-zeros-after-decimal-point-in-bigdecimal
            // 200.000 stripTrailingZeros => 2E+2
            // 200.000 stripTrailingZeros.toPlainString => 200
            return decimal.stripTrailingZeros().toPlainString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
            BigDecimal bigDecimal = new BigDecimal(str);
            int valueIntLength = bigDecimal.precision() - bigDecimal.scale();
            int typeIntLength = decimalTypeInfo.getPrecision() - decimalTypeInfo.getScale();
            if (valueIntLength > typeIntLength) {
                throw new IllegalArgumentException(
                        String.format(
                                "InvalidData: decimal value %s overflow, max integer digit number is %s.",
                                str,
                                typeIntLength));
            }
            // tunnel is half_down mode
            bigDecimal = bigDecimal.setScale(decimalTypeInfo.getScale(), RoundingMode.HALF_DOWN);
            return bigDecimal;
        }

    }

    private enum CharConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return object.toString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return new Char(str);
        }

    }

    private enum VarcharConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return object.toString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return new Varchar(str);
        }

    }

    private enum StringConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (object instanceof byte[]) {
                try {
                    return new String((byte[]) object, "utf8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            } else {
                return (String) object;
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return str;
        }

    }

    private enum BooleanConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return object.toString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return Boolean.parseBoolean(str);
        }

    }

    private enum BinaryConverter implements OdpsObjectConverter {
        BASE64(BINARY_FORMAT_BASE64),
        UTF8(BINARY_FORMAT_UTF8),
        QUOTED_PRINTABLE(BINARY_FORMAT_QUOTED_PRINTABLE);

        private String type;

        BinaryConverter(String type) {
            this.type = type.toLowerCase();
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            Binary binary = (Binary) object;
            byte[] bytes = binary.data();
            switch (type) {
                case BINARY_FORMAT_UTF8:
                    return new String(bytes, StandardCharsets.UTF_8);
                case BINARY_FORMAT_BASE64:
                    return Base64.getEncoder().encodeToString(bytes);
                case BINARY_FORMAT_QUOTED_PRINTABLE:
                    return binary.toString();
                default:
                    throw new RuntimeException("Unsupported binary encode type: " + type);
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            switch (type) {
                case ConverterConstant.BINARY_FORMAT_UTF8:
                    return str.getBytes(StandardCharsets.UTF_8);
                case ConverterConstant.BINARY_FORMAT_BASE64:
                    return Base64.getDecoder().decode(str);
                default:
                    throw new IllegalArgumentException();
            }
        }
    }

    private enum JsonConverter implements OdpsObjectConverter {
        INSTANCE;

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return object.toString();
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return new SimpleJsonValue(str);
        }
    }


    private static class DateConverter implements OdpsObjectConverter {

        // u - year, y - year-of-era(not support 0000)
        // use uuuu to support year 0000

        // sql is yyyy-MM-dd
        private final static String DEFAULT_OUTPUT_PATTERN = "uuuu-MM-dd";
        // sql is yyyy-M-d
        private final static String DEFAULT_PARSE_PATTERN = "uuuu-M-d";

        private DateTimeFormatter outputFormatter;
        private DateTimeFormatter parseFormatter;
        private SimpleDateFormat legacyOutputFormatter;
        private SimpleDateFormat legacyParseFormatter;
        private boolean isLegacyType;


        DateConverter(String outputPattern, String parsePattern, boolean isLegacyType) {
            this.isLegacyType = isLegacyType;
            if (null == outputPattern) {
                outputPattern = DEFAULT_OUTPUT_PATTERN;
            }
            if (null == parsePattern) {
                parsePattern = DEFAULT_PARSE_PATTERN;
            }
            if (isLegacyType) {
                legacyOutputFormatter = getLegacyDateTimeFormatter(outputPattern, OdpsType.DATE);
                legacyParseFormatter = getLegacyDateTimeFormatter(parsePattern, OdpsType.DATE);
            } else {
                outputFormatter = getDateTimeFormatter(outputPattern, null, OdpsType.DATE);
                parseFormatter = getDateTimeFormatter(parsePattern, null, OdpsType.DATE);
            }
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (isLegacyType) {
                Date date = (Date) object;
                synchronized (legacyOutputFormatter) {
                    return legacyOutputFormatter.format(date);
                }
            } else {
                LocalDate localDate = (LocalDate) object;
                return localDate.format(outputFormatter);
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (isLegacyType) {
                try {
                    java.util.Date date = legacyParseFormatter.parse(str);
                    return new Date(date.getTime());
                } catch (ParseException e) {
                    throw new DateTimeParseException(null, str, 0, e);
                }
            } else {
                return LocalDate.parse(str, parseFormatter);
            }
        }
    }

    private static class DatetimeConverter implements OdpsObjectConverter {

        // SQL is yyyy-MM-dd HH:mm:ss
        private final static String DEFAULT_OUTPUT_PATTERN = "uuuu-MM-dd HH:mm:ss";

        private final static String DEFAULT_PARSE_PATTERN = "uuuu-M-d H:m:s";
        private final static DateTimeFormatter DEFAULT_PARSER = new DateTimeFormatterBuilder()
                .appendPattern(DEFAULT_PARSE_PATTERN)
                .optionalStart()
                .appendFraction(NANO_OF_SECOND, 0, 9, true)
                .toFormatter()
                .withResolverStyle(ResolverStyle.LENIENT);

        private DateTimeFormatter outputFormatter;
        private DateTimeFormatter parseFormatter;
        private SimpleDateFormat legacyOutputFormatter;
        private SimpleDateFormat legacyParseFormatter;
        private boolean isLegacyType;

        DatetimeConverter(String outputPattern, String parsePattern, ZoneId zoneId,
                          boolean isLegacyType) {
            this.isLegacyType = isLegacyType;
            if (null == outputPattern) {
                outputPattern = DEFAULT_OUTPUT_PATTERN;
            }
            if (null == parsePattern) {
                parsePattern = DEFAULT_PARSE_PATTERN;
                parseFormatter = DEFAULT_PARSER.withZone(zoneId);
            } else {
                parseFormatter = getDateTimeFormatter(parsePattern, zoneId, OdpsType.DATETIME);
            }
            if (isLegacyType) {
                legacyOutputFormatter = getLegacyDateTimeFormatter(outputPattern, OdpsType.DATETIME);
                legacyParseFormatter = getLegacyDateTimeFormatter(parsePattern, OdpsType.DATETIME);
            } else {
                outputFormatter = getDateTimeFormatter(outputPattern, zoneId, OdpsType.DATETIME);
            }
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (isLegacyType) {
                java.util.Date date = (java.util.Date) object;
                synchronized (legacyOutputFormatter) {
                    return legacyOutputFormatter.format(date);
                }
            } else {
                ZonedDateTime zdt = (ZonedDateTime) object;
                return zdt.format(outputFormatter);
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (isLegacyType) {
                try {
                    return legacyParseFormatter.parse(str);
                } catch (ParseException e) {
                    throw new DateTimeParseException(null, str, 0, e);
                }
            } else {
                ZonedDateTime zdt = ZonedDateTime.parse(str, parseFormatter);
                Instant instant = zdt.toInstant();
                instant = instant.minusNanos(instant.getNano() % 1000000);
                return ZonedDateTime.ofInstant(instant, parseFormatter.getZone());
            }
        }
    }

    private static abstract class AbstractTimestampFormatter implements OdpsObjectConverter {

        DateTimeFormatter outputFormatter = new DateTimeFormatterBuilder()
                .appendPattern("uuuu-MM-dd HH:mm:ss")
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .toFormatter();

        // same as odps sql BuiltInDateTimeFunctionProvider#timestampParseFormat
        // but yyyy -> uuuu
        DateTimeFormatter parseFormatter =
                new DateTimeFormatterBuilder()
                        .appendPattern("uuuu-M-d H:m:s")
                        .optionalStart()
                        .appendFraction(NANO_OF_SECOND, 0, 9, true)
                        .toFormatter()
                        .withResolverStyle(ResolverStyle.LENIENT);

    }

    private static class TimestampConverter extends AbstractTimestampFormatter {

        private boolean isLegacyType;

        TimestampConverter(String outputPattern, String parsePattern, ZoneId timezone, boolean isLegacyType) {
            this.isLegacyType = isLegacyType;
            if (outputPattern != null) {
                outputFormatter = getDateTimeFormatter(outputPattern, timezone, OdpsType.TIMESTAMP);
            } else {
                outputFormatter = outputFormatter.withZone(timezone);
            }

            if (parsePattern != null) {
                parseFormatter = getDateTimeFormatter(parsePattern, timezone, OdpsType.TIMESTAMP);
            } else {
                parseFormatter = parseFormatter.withZone(timezone);
            }

        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (isLegacyType) {
                Timestamp timestamp = (Timestamp) object;
                String output = timestamp.toString();
                return timestamp.getNanos() == 0 ? output.substring(0, output.length() - 2) : output;
            }
            Instant i = (Instant) object;
            return outputFormatter.format(i);
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            if (isLegacyType) {
                return Timestamp.valueOf(str);
            } else {
                return ZonedDateTime.parse(str, parseFormatter).toInstant();
            }
        }
    }

    private static class TimestampNtzConverter extends AbstractTimestampFormatter {

        TimestampNtzConverter(String outputPattern, String parsePattern) {
            if (outputPattern != null) {
                outputFormatter = getDateTimeFormatter(outputPattern, null, OdpsType.TIMESTAMP_NTZ);

            }
            if (parsePattern != null) {
                parseFormatter = getDateTimeFormatter(outputPattern, null, OdpsType.TIMESTAMP_NTZ);
            }
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            LocalDateTime localDateTime = (LocalDateTime) object;
            return localDateTime.format(outputFormatter);
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return LocalDateTime.parse(str, parseFormatter);
        }
    }

    static OdpsObjectConverter getFormatter(OdpsType odpsType, OdpsRecordConverterBuilder.Config config) {
        switch (odpsType) {
            case TINYINT:
                return TinyIntConverter.INSTANCE;
            case SMALLINT:
                return SmallIntConverter.INSTANCE;
            case INT:
                return IntConverter.INSTANCE;
            case BIGINT:
                return BigIntConverter.INSTANCE;
            case CHAR:
                return CharConverter.INSTANCE;
            case VARCHAR:
                return VarcharConverter.INSTANCE;
            case STRING:
                return StringConverter.INSTANCE;
            case FLOAT:
                switch (config.floatingNumberOutputFormat) {
                    case FLOATING_NUMBER_OUTPUT_FORMAT_TO_STRING:
                        return FloatConverter.TO_STRING;
                    case FLOATING_NUMBER_OUTPUT_FORMAT_SQL_COMPATIBLE:
                        return FloatConverter.SQL_COMPATIBLE;
                    default:
                        throw new IllegalArgumentException("unsupported float format type");
                }
            case JSON:
                return JsonConverter.INSTANCE;
            case BOOLEAN:
                return BooleanConverter.INSTANCE;
            case DOUBLE:
                switch (config.floatingNumberOutputFormat) {
                    case FLOATING_NUMBER_OUTPUT_FORMAT_TO_STRING:
                        return DoubleConverter.TO_STRING;
                    case FLOATING_NUMBER_OUTPUT_FORMAT_SQL_COMPATIBLE:
                        return DoubleConverter.SQL_COMPATIBLE;
                    default:
                        throw new IllegalArgumentException("unsupported double format type");
                }
            case DECIMAL:
                switch (config.decimalOutputFormat) {
                    case DECIMAL_OUTPUT_FORMAT_NORMAL:
                        return DecimalConverter.NO_PADDING;
                    case DECIMAL_OUTPUT_FORMAT_ZERO_PADDING:
                        return DecimalConverter.PADDING;
                    default:
                        throw new IllegalArgumentException("unsupported decimal format type");
                }
            case DATE:
                return new DateConverter(config.dateOutputFormat, config.dateParseFormat, config.legacyTimeType);
            case DATETIME:
                return new DatetimeConverter(config.datetimeOutputFormat, config.datetimeParseFormat, config.timezone, config.legacyTimeType);
            case TIMESTAMP:
                return new TimestampConverter(config.timestampOutputFormat, config.timestampParseFormat,
                        config.timezone, config.legacyTimeType);
            case TIMESTAMP_NTZ:
                return new TimestampNtzConverter(config.timestampNtzOutputFormat, config.timestampNtzParseFormat);
            case BINARY:
                switch (config.binaryFormat) {
                    case BINARY_FORMAT_BASE64:
                        return BinaryConverter.BASE64;
                    case BINARY_FORMAT_UTF8:
                        return BinaryConverter.UTF8;
                    case BINARY_FORMAT_QUOTED_PRINTABLE:
                        return BinaryConverter.QUOTED_PRINTABLE;
                }
            case ARRAY:
            case MAP:
            case STRUCT:
                switch (config.complexTypeFormat) {
                    case COMPLEX_TYPE_FORMAT_JSON:
                        return ComplexObjectConverter.JSON;
                    case COMPLEX_TYPE_FORMAT_JSON_STR:
                        return ComplexObjectConverter.JSON_ALL_STR;
                    case COMPLEX_TYPE_OUTPUT_FORMAT_HUMAN_READABLE:
                        return ComplexObjectConverter.HUMAN_READABLE;
                }
            default:
                throw new IllegalArgumentException("unsupported data type");
        }
    }

    private static DateTimeFormatter getDateTimeFormatter(String pattern, ZoneId zoneId, OdpsType odpsType) {
        try {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
            if (zoneId != null) {
                dateTimeFormatter = dateTimeFormatter.withZone(zoneId);
            }
            return dateTimeFormatter.withResolverStyle(ResolverStyle.LENIENT);
        } catch (Exception e) {
            throw new IllegalArgumentException("DateTime format for " + odpsType + " illegal: " + pattern);
        }
    }

    private static SimpleDateFormat getLegacyDateTimeFormatter(String pattern, OdpsType odpsType) {
        try {
            pattern = pattern.replace("u", "y");
            return new SimpleDateFormat(pattern);
        } catch (Exception e) {
            throw new IllegalArgumentException("DateTime format for " + odpsType + " illegal: " + pattern);
        }
    }

    private static boolean isNan(String value) {
        return value.equalsIgnoreCase("nan") ||
                value.equalsIgnoreCase("+nan") ||
                value.equalsIgnoreCase("-nan");
    }

    private static boolean isPositiveInfinity(String value) {
        return value.equalsIgnoreCase("infinity") ||
                value.equalsIgnoreCase("+infinity") ||
                value.equalsIgnoreCase("inf") ||
                value.equalsIgnoreCase("+inf");
    }

    static boolean isNegativeInfinity(String value) {
        return value.equalsIgnoreCase("-infinity") ||
                value.equalsIgnoreCase("-inf");
    }
}
