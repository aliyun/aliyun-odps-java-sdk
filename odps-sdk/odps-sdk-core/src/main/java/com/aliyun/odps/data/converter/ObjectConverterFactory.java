package com.aliyun.odps.data.converter;

import static com.aliyun.odps.data.converter.ConverterConstant.BINARY_FORMAT_BASE64;
import static com.aliyun.odps.data.converter.ConverterConstant.BINARY_FORMAT_HEX;
import static com.aliyun.odps.data.converter.ConverterConstant.BINARY_FORMAT_QUOTED_PRINTABLE;
import static com.aliyun.odps.data.converter.ConverterConstant.BINARY_FORMAT_SQL_FORMAT;
import static com.aliyun.odps.data.converter.ConverterConstant.BINARY_FORMAT_UTF8;
import static com.aliyun.odps.data.converter.ConverterConstant.COMPLEX_TYPE_FORMAT_JSON;
import static com.aliyun.odps.data.converter.ConverterConstant.COMPLEX_TYPE_FORMAT_JSON_STR;
import static com.aliyun.odps.data.converter.ConverterConstant.COMPLEX_TYPE_OUTPUT_FORMAT_HUMAN_READABLE;
import static com.aliyun.odps.data.converter.ConverterConstant.DECIMAL_OUTPUT_FORMAT_NORMAL;
import static com.aliyun.odps.data.converter.ConverterConstant.DECIMAL_OUTPUT_FORMAT_ZERO_PADDING;
import static com.aliyun.odps.data.converter.ConverterConstant.FLOATING_NUMBER_OUTPUT_FORMAT_SQL_COMPATIBLE;
import static com.aliyun.odps.data.converter.ConverterConstant.FLOATING_NUMBER_OUTPUT_FORMAT_TO_STRING;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Base64;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.SimpleJsonValue;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;

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
            if (object instanceof BigDecimal) {
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
            } else if (object instanceof Number) {
                return object.toString();
            } else {
                throw new IllegalArgumentException(
                    "Unsupported DECIMAL object type: " + object.getClass());
            }
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
        INSTANCE(false),
        QUOTE_INSTANCE(true);
        private final boolean quoteStrings;

        CharConverter(boolean quoteStrings) {
            this.quoteStrings = quoteStrings;
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            String str;
            if (object instanceof byte[]) {
                str = new String((byte[]) object, StandardCharsets.UTF_8);
            } else {
                str = object.toString();
            }
            if (quoteStrings) {
                String escapedString = str.replace("'", "\\'");
                return "'" + escapedString + "'";
            } else {
                return str;
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return new Char(str);
        }

    }

    private enum VarcharConverter implements OdpsObjectConverter {
        INSTANCE(false),
        QUOTE_INSTANCE(true);
        private final boolean quoteStrings;

        VarcharConverter(boolean quoteStrings) {
            this.quoteStrings = quoteStrings;
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            String str;
            if (object instanceof byte[]) {
                str = new String((byte[]) object, StandardCharsets.UTF_8);
            } else {
                str = object.toString();
            }
            if (quoteStrings) {
                String escapedString = str.replace("'", "\\'");
                return "'" + escapedString + "'";
            } else {
                return str;
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return new Varchar(str);
        }

    }

    private enum StringConverter implements OdpsObjectConverter {
        INSTANCE(false),
        QUOTE_INSTANCE(true);
        private final boolean quoteStrings;

        StringConverter(boolean quoteStrings) {
            this.quoteStrings = quoteStrings;
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            String str;
            if (object instanceof byte[]) {
                str = new String((byte[]) object, StandardCharsets.UTF_8);
            } else {
                str = (String) object;
            }
            if (quoteStrings) {
                String escapedString = str.replace("'", "\\'");
                return "'" + escapedString + "'";
            } else {
                return str;
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
        HEX(BINARY_FORMAT_HEX),
        SQL_FORMAT(BINARY_FORMAT_SQL_FORMAT),
        UTF8(BINARY_FORMAT_UTF8),
        QUOTED_PRINTABLE(BINARY_FORMAT_QUOTED_PRINTABLE);

        private String type;

        BinaryConverter(String type) {
            this.type = type.toLowerCase();
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            byte[] bytes;
            if (object instanceof Binary) {
                Binary binary = (Binary) object;
                bytes = binary.data();
            } else if (object instanceof byte[]) {
                bytes = (byte[]) object;
            } else {
                throw new IllegalArgumentException(
                    "Unsupported BINARY object type: " + object.getClass());
            }
            switch (type) {
                case BINARY_FORMAT_UTF8:
                    return new String(bytes, StandardCharsets.UTF_8);
                case BINARY_FORMAT_BASE64:
                    return Base64.getEncoder().encodeToString(bytes);
                case BINARY_FORMAT_QUOTED_PRINTABLE:
                    return new Binary(bytes).toString();
                case BINARY_FORMAT_HEX:
                    return new String(Hex.encodeHex(bytes));
                case BINARY_FORMAT_SQL_FORMAT:
                    return "X'" + new String(Hex.encodeHex(bytes)) + "'";
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
                case ConverterConstant.BINARY_FORMAT_HEX:
                    try {
                        return Hex.decodeHex(str.toCharArray());
                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                            String.format("InvalidData: invalid hex string %s", str));
                    }
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
        private static final String DEFAULT_OUTPUT_PATTERN = "uuuu-MM-dd";
        // sql is yyyy-M-d
        private static final String DEFAULT_PARSE_PATTERN = "uuuu-M-d";

        private DateTimeFormatter outputFormatter;
        private DateTimeFormatter parseFormatter;
        private SimpleDateFormat legacyOutputFormatter;
        private ZoneId zoneId;
        private final boolean strictMode;
        private final boolean useSqlFormat;


        DateConverter(OdpsRecordConverterBuilder.Config config) {
            this.strictMode = config.strictMode;
            this.useSqlFormat = config.useSqlFormat;
            this.zoneId = config.timezone;

            String outputPattern = config.dateOutputFormat;
            if (null == outputPattern) {
                outputPattern = DEFAULT_OUTPUT_PATTERN;
            }
            String parsePattern = config.dateParseFormat;
            if (null == parsePattern) {
                parsePattern = DEFAULT_PARSE_PATTERN;
            }
            if (!strictMode) {
                legacyOutputFormatter =
                    getLegacyDateTimeFormatter(outputPattern, config.timezone, OdpsType.DATE);
            }
            outputFormatter = getDateTimeFormatter(outputPattern, config.timezone, OdpsType.DATE);
            parseFormatter = getDateTimeFormatter(parsePattern, config.timezone, OdpsType.DATE);
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            String formattedStr;
            if (object instanceof LocalDate) {
                LocalDate localDate = (LocalDate) object;
                formattedStr = localDate.format(outputFormatter);
            } else if (object instanceof ZonedDateTime) {
                ZonedDateTime zdt = (ZonedDateTime) object;
                formattedStr = zdt.format(outputFormatter);
            } else if (object instanceof java.util.Date) {
                java.util.Date date = (java.util.Date) object;
                synchronized (legacyOutputFormatter) {
                    formattedStr = legacyOutputFormatter.format(date);
                }
            } else if (object instanceof LocalDateTime) {
                LocalDateTime localDateTime = (LocalDateTime) object;
                formattedStr = localDateTime.format(outputFormatter);
            } else if (object instanceof Instant) {
                Instant instant = (Instant) object;
                formattedStr = instant.atZone(zoneId).format(outputFormatter);
            } else {
                throw new IllegalArgumentException(
                    "Unsupported DATE object type: " + object.getClass());
            }
            if (useSqlFormat) {
                return "DATE'" + formattedStr + "'";
            } else {
                return formattedStr;
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return LocalDate.parse(str, parseFormatter);
        }
    }

    private static class DatetimeConverter implements OdpsObjectConverter {

        // SQL is yyyy-MM-dd HH:mm:ss
        private static final String DEFAULT_OUTPUT_PATTERN = "uuuu-MM-dd HH:mm:ss";

        private static final String DEFAULT_PARSE_PATTERN = "uuuu-M-d H:m:s";
        private static final DateTimeFormatter DEFAULT_PARSER = new DateTimeFormatterBuilder()
                .appendPattern(DEFAULT_PARSE_PATTERN)
                .optionalStart()
                .appendFraction(NANO_OF_SECOND, 0, 9, true)
                .toFormatter()
                .withResolverStyle(ResolverStyle.LENIENT);

        private DateTimeFormatter outputFormatter;
        private DateTimeFormatter parseFormatter;
        private SimpleDateFormat legacyOutputFormatter;
        private final boolean strictMode;
        private final boolean useSqlFormat;

        DatetimeConverter(OdpsRecordConverterBuilder.Config config) {
            this.strictMode = config.strictMode;
            this.useSqlFormat = config.useSqlFormat;

            String outputPattern = config.datetimeOutputFormat;
            if (null == outputPattern) {
                outputPattern = DEFAULT_OUTPUT_PATTERN;
            }
            String parsePattern = config.datetimeParseFormat;
            if (null == parsePattern) {
                parseFormatter = DEFAULT_PARSER.withZone(config.timezone);
            } else {
                parseFormatter = getDateTimeFormatter(parsePattern, config.timezone, OdpsType.DATETIME);
            }
            if (!strictMode) {
                legacyOutputFormatter = getLegacyDateTimeFormatter(outputPattern, config.timezone, OdpsType.DATETIME);
            }
            outputFormatter = getDateTimeFormatter(outputPattern, config.timezone, OdpsType.DATETIME);
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            String formattedStr;
            if (object instanceof ZonedDateTime) {
                ZonedDateTime zdt = (ZonedDateTime) object;
                formattedStr = zdt.format(outputFormatter);
            } else if (object instanceof java.util.Date) {
                java.util.Date date = (java.util.Date) object;
                synchronized (legacyOutputFormatter) {
                    formattedStr = legacyOutputFormatter.format(date);
                }
            } else if (object instanceof LocalDateTime) {
                LocalDateTime localDateTime = (LocalDateTime) object;
                formattedStr = localDateTime.format(outputFormatter);
            } else if (object instanceof Instant) {
                Instant instant = (Instant) object;
                formattedStr = instant.atZone(outputFormatter.getZone()).format(outputFormatter);
            } else {
                throw new IllegalArgumentException(
                    "Unsupported DATETIME object type: " + object.getClass());
            }
            if (useSqlFormat) {
                return "DATETIME'" + formattedStr + "'";
            } else {
                return formattedStr;
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            ZonedDateTime zdt = ZonedDateTime.parse(str, parseFormatter);
            Instant instant = zdt.toInstant();
            instant = instant.minusNanos(instant.getNano() % 1000000);
            return ZonedDateTime.ofInstant(instant, parseFormatter.getZone());
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
        private final boolean useSqlFormat;
        private final ZoneId timezone;
        TimestampConverter(OdpsRecordConverterBuilder.Config config) {
            this.useSqlFormat = config.useSqlFormat;
            this.timezone = config.timezone;
            String outputPattern = config.timestampOutputFormat;
            if (outputPattern != null) {
                outputFormatter =
                    getDateTimeFormatter(outputPattern, config.timezone, OdpsType.TIMESTAMP);
            } else {
                outputFormatter = outputFormatter.withZone(config.timezone);
            }
            String parsePattern = config.timestampParseFormat;
            if (parsePattern != null) {
                parseFormatter =
                    getDateTimeFormatter(parsePattern, config.timezone, OdpsType.TIMESTAMP);
            } else {
                parseFormatter = parseFormatter.withZone(config.timezone);
            }
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            String formattedStr;
            if (object instanceof Instant) {
                Instant i = (Instant) object;
                formattedStr = outputFormatter.format(i);
            } else if (object instanceof Timestamp) {
                Timestamp timestamp = (Timestamp) object;
                formattedStr = outputFormatter.format(timestamp.toInstant().atZone(timezone));
            } else if (object instanceof LocalDateTime) {
                LocalDateTime localDateTime = (LocalDateTime) object;
                formattedStr = localDateTime.format(outputFormatter);
            } else if (object instanceof ZonedDateTime) {
                ZonedDateTime zdt = (ZonedDateTime) object;
                formattedStr = zdt.format(outputFormatter);
            } else {
                throw new IllegalArgumentException(
                    "Unsupported TIMESTAMP object type: " + object.getClass());
            }
            if (useSqlFormat) {
                return "TIMESTAMP'" + formattedStr + "'";
            } else {
                return formattedStr;
            }
        }

        @Override
        public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
            return ZonedDateTime.parse(str, parseFormatter).toInstant();
        }
    }

    private static class TimestampNtzConverter extends AbstractTimestampFormatter {

        private final boolean useSqlFormat;

        /**
         * note that although timestamp_ntz is of no time zone type,
         * the purpose of using zoneId here is to format the time zone type data passed in by the user, such as Instant
         */
        private final ZoneId zoneId;

        TimestampNtzConverter(OdpsRecordConverterBuilder.Config config) {
            this.useSqlFormat = config.useSqlFormat;
            this.zoneId = config.timezone;

            String outputPattern = config.timestampNtzOutputFormat;
            if (outputPattern != null) {
                outputFormatter = getDateTimeFormatter(outputPattern, ZoneId.of("UTC"), OdpsType.TIMESTAMP_NTZ);
            }
            String parsePattern = config.timestampNtzParseFormat;
            if (parsePattern != null) {
                parseFormatter = getDateTimeFormatter(outputPattern, ZoneId.of("UTC"), OdpsType.TIMESTAMP_NTZ);
            }
        }

        @Override
        public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
            String formattedStr;
            if (object instanceof Instant) {
                Instant i = (Instant) object;
                formattedStr = i.atZone(zoneId).format(outputFormatter);
            } else if (object instanceof Timestamp) {
                Instant i = ((Timestamp) object).toInstant();
                formattedStr = i.atZone(zoneId).format(outputFormatter);
            } else if (object instanceof LocalDateTime) {
                LocalDateTime localDateTime = (LocalDateTime) object;
                formattedStr = localDateTime.format(outputFormatter);
            } else if (object instanceof ZonedDateTime) {
                ZonedDateTime zdt = (ZonedDateTime) object;
                formattedStr = zdt.format(outputFormatter);
            } else {
                throw new IllegalArgumentException(
                    "Unsupported TIMESTAMP_NTZ object type: " + object.getClass());
            }
            if (useSqlFormat) {
                return "TIMESTAMP_NTZ'" + formattedStr + "'";
            } else {
                return formattedStr;
            }
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
                return config.quoteStrings ? CharConverter.QUOTE_INSTANCE : CharConverter.INSTANCE;
            case VARCHAR:
                return config.quoteStrings ? VarcharConverter.QUOTE_INSTANCE
                                           : VarcharConverter.INSTANCE;
            case STRING:
                return config.quoteStrings ? StringConverter.QUOTE_INSTANCE
                                           : StringConverter.INSTANCE;
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
                return new DateConverter(config);
            case DATETIME:
                return new DatetimeConverter(config);
            case TIMESTAMP:
                return new TimestampConverter(config);
            case TIMESTAMP_NTZ:
                return new TimestampNtzConverter(config);
            case BINARY:
                switch (config.binaryFormat) {
                    case BINARY_FORMAT_BASE64:
                        return BinaryConverter.BASE64;
                    case BINARY_FORMAT_UTF8:
                        return BinaryConverter.UTF8;
                    case BINARY_FORMAT_QUOTED_PRINTABLE:
                        return BinaryConverter.QUOTED_PRINTABLE;
                    case BINARY_FORMAT_HEX:
                        return BinaryConverter.HEX;
                    case BINARY_FORMAT_SQL_FORMAT:
                        return BinaryConverter.SQL_FORMAT;
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

    private static SimpleDateFormat getLegacyDateTimeFormatter(String pattern, ZoneId zoneId,
                                                               OdpsType odpsType) {
        try {
            pattern = pattern.replace("u", "y");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            if (zoneId != null) {
                simpleDateFormat.setTimeZone(TimeZone.getTimeZone(zoneId));
            }
            return simpleDateFormat;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "DateTime format for " + odpsType + " illegal: " + pattern);
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
