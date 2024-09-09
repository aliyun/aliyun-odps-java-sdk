package com.aliyun.odps.data.converter;

import static com.aliyun.odps.data.converter.ConverterConstant.COMPLEX_TYPE_FORMAT_JSON;
import static com.aliyun.odps.data.converter.ConverterConstant.COMPLEX_TYPE_FORMAT_JSON_STR;
import static com.aliyun.odps.data.converter.ConverterConstant.COMPLEX_TYPE_OUTPUT_FORMAT_HUMAN_READABLE;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;

/**
 * Use this builder to construct a {@link OdpsRecordConverter} instance when you need to set configuration
 * options other than the default. For {@link OdpsRecordConverter} with default configuration, it is simpler
 * to use {@link OdpsRecordConverter#defaultConverter()}
 * <p>
 * The following is an example shows how to use the OdpsRecordConverterBuilder to construct a OdpsRecordConverter instance.
 * <pre>
 *     {@code
 *     OdpsRecordConverter formatter = OdpsRecordConverter.builder()
 *         .decimalFormatWithZeroPadding()
 *         .dateFormat("MM-dd-yyyy")
 *         .build();
 *     }
 * </pre>
 */
public class OdpsRecordConverterBuilder {

    private final Config config = new Config();

    static class Config {

        String decimalOutputFormat = ConverterConstant.DECIMAL_OUTPUT_FORMAT_DEFAULT;
        String floatingNumberOutputFormat = ConverterConstant.FLOATING_NUMBER_OUTPUT_FORMAT_DEFAULT;
        String dateParseFormat;
        String dateOutputFormat;
        String datetimeParseFormat;
        String datetimeOutputFormat;
        String timestampParseFormat;
        String timestampOutputFormat;
        String timestampNtzParseFormat;
        String timestampNtzOutputFormat;
        ZoneId timezone = ZoneId.systemDefault();
        String complexTypeFormat = ConverterConstant.COMPLEX_TYPE_FORMAT_DEFAULT;
        String binaryFormat = ConverterConstant.BINARY_FORMAT_DEFAULT;
        String nullFormat = ConverterConstant.NULL_OUTPUT_FORMAT_DEFAULT;
        boolean enableNullParse = false;
        boolean strictMode = true;
        boolean quoteStrings = false;
        boolean useSqlFormat = false;
    }

    private Function<Column[], Record> recordProvider;
    private final Map<OdpsType, OdpsObjectConverter> formatterMap = new HashMap<>();

    OdpsRecordConverterBuilder() {
    }


    /**
     * Configure OdpsRecordConverter to enable SQL format mode.
     * <p>
     * After enabling this mode, the converter will convert data types such as dates into string formats that comply with SQL statement standards.
     * For example, convert date to 'DATE'YYYY-MM-DD' so that it can be used directly to construct SQL statements.
     *
     * @return this     */
    public OdpsRecordConverterBuilder enableSqlStandardFormat() {
        this.config.useSqlFormat = true;
        this.config.quoteStrings = true;
        this.config.binaryFormat = ConverterConstant.BINARY_FORMAT_SQL_FORMAT;
        return this;
    }

    /**
     * Configure OdpsRecordConverter to enable string quoting mode.
     * <p>
     * When this mode is enabled, the converter automatically adds single quotes around string data types when processing them.
     * For example, the original string "a" will be converted to "'a'" to ensure correct parsing as a string literal in the SQL statement.
     *
     * @return this
     */
    public OdpsRecordConverterBuilder enableQuoteString() {
        this.config.quoteStrings = true;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format {@link OdpsType#FLOAT}, {@link OdpsType#DOUBLE}
     * in same pattern with Odps SQL, {@link com.aliyun.odps.task.SQLTask#getResult(Instance)}.
     *
     * <p> Format performance will be worse than {@link #floatingNumberFormatToString()},
     * only for compatible usage
     * <p>
     * This configuration has no impact on object parse
     *
     * @return this
     */
    public OdpsRecordConverterBuilder floatingNumberFormatCompatible() {
        this.config.floatingNumberOutputFormat = ConverterConstant.FLOATING_NUMBER_OUTPUT_FORMAT_SQL_COMPATIBLE;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format {@link OdpsType#FLOAT}, {@link OdpsType#DOUBLE}
     *  like {@link Float#toString(float)}, {@link Double#toString(double)}
     *
     * This configuration has no impact on object parse
     *
     * @return this
     */
    public OdpsRecordConverterBuilder floatingNumberFormatToString() {
        this.config.floatingNumberOutputFormat = ConverterConstant.FLOATING_NUMBER_OUTPUT_FORMAT_TO_STRING;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format {@link OdpsType#DECIMAL} with padding trailing zeros.
     * This configuration has no impact on object parse
     * <p>
     * example:
     *   type: Decimal(10,3), value: 1.1 => format result: 1.100
     *
     * @return this
     */
    public OdpsRecordConverterBuilder decimalFormatWithZeroPadding() {
        this.config.decimalOutputFormat = ConverterConstant.DECIMAL_OUTPUT_FORMAT_ZERO_PADDING;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format {@link OdpsType#DECIMAL} without padding trailing zeros.
     * This configuration has no impact on object parse
     * <p>
     * example:
     *   type: Decimal(10,3), value: 1.1 => format result: 1.1
     *
     * @return this
     */
    public OdpsRecordConverterBuilder decimalFormatWithoutZeroPadding() {
        this.config.decimalOutputFormat = ConverterConstant.DECIMAL_OUTPUT_FORMAT_NORMAL;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#DATE} according to the format pattern provided.
     * <p>
     * OdpsRecordConverter will format/parse {@link OdpsType#DATE} by
     * <code>DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.LENIENT)</code>
     *
     * @param format date format pattern that date will be formatted/parsed
     * @return this
     */
    public OdpsRecordConverterBuilder dateFormat(String format) {
        this.config.dateParseFormat = format;
        this.config.dateOutputFormat = format;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#DATE} according to the format pattern provided.
     * <p>
     * OdpsRecordConverter will format/parse {@link OdpsType#DATE} by
     * <code>DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.LENIENT)</code>
     *
     * @param parseFormat date format pattern that date will be parsed
     * @param outputFormat date format pattern that date will be formatted
     * @return this
     */
    public OdpsRecordConverterBuilder dateFormat(String parseFormat, String outputFormat) {
        this.config.dateParseFormat = parseFormat;
        this.config.dateOutputFormat = outputFormat;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#DATETIME} according to the format pattern provided.
     * <p>
     * OdpsRecordConverter will format/parse {@link OdpsType#DATETIME} by
     * <code>DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.LENIENT)</code>
     *
     * @param format datetime format pattern that date will be formatted/parsed
     * @return this
     */
    public OdpsRecordConverterBuilder datetimeFormat(String format) {
        this.config.datetimeOutputFormat = format;
        this.config.datetimeParseFormat = format;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#DATETIME} according to the format pattern provided.
     * <p>
     * OdpsRecordConverter will format/parse {@link OdpsType#DATETIME} by
     * <code>DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.LENIENT)</code>
     *
     * @param parseFormat datetime format pattern that date will be parsed
     * @param outputFormat datetime format pattern that date will be formatted
     * @return this
     */
    public OdpsRecordConverterBuilder datetimeFormat(String parseFormat, String outputFormat) {
        this.config.datetimeOutputFormat = outputFormat;
        this.config.datetimeParseFormat = parseFormat;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#TIMESTAMP} according to the format pattern provided.
     * <p>
     * OdpsRecordConverter will format/parse {@link OdpsType#TIMESTAMP} by
     * <code>DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.LENIENT)</code>
     *
     * @param format timestamp format pattern that date will be formatted/parsed
     * @return this
     */
    public OdpsRecordConverterBuilder timestampFormat(String format) {
        this.config.timestampOutputFormat = format;
        this.config.timestampParseFormat = format;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#TIMESTAMP} according to the format pattern provided.
     * <p>
     * OdpsRecordConverter will format/parse {@link OdpsType#TIMESTAMP} by
     * <code>DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.LENIENT)</code>
     *
     * @param parseFormat timestamp format pattern that date will be parsed
     * @param outputFormat timestamp format pattern that date will be formatted
     * @return this
     */
    public OdpsRecordConverterBuilder timestampFormat(String parseFormat, String outputFormat) {
        this.config.timestampOutputFormat = outputFormat;
        this.config.timestampParseFormat = parseFormat;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#TIMESTAMP_NTZ} according to the format pattern provided.
     * <p>
     * OdpsRecordConverter will format/parse {@link OdpsType#TIMESTAMP_NTZ} by
     * <code>DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.LENIENT)</code>
     *
     * @param format timestamp format pattern that date will be formatted/parsed
     * @return this
     */
    public OdpsRecordConverterBuilder timestampNtzFormat(String format) {
        this.config.timestampNtzOutputFormat = format;
        this.config.timestampNtzParseFormat = format;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#TIMESTAMP_NTZ} according to the format pattern provided.
     * <p>
     * OdpsRecordConverter will format/parse {@link OdpsType#TIMESTAMP_NTZ} by
     * <code>DateTimeFormatter.ofPattern(pattern).withResolverStyle(ResolverStyle.LENIENT)</code>
     *
     * @param parseFormat timestamp format pattern that date will be parsed
     * @param outputFormat timestamp format pattern that date will be formatted
     * @return this
     */
    public OdpsRecordConverterBuilder timestampNtzFormat(String parseFormat, String outputFormat) {
        this.config.timestampNtzParseFormat = parseFormat;
        this.config.timestampNtzOutputFormat = outputFormat;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#DATETIME}
     * {@link OdpsType#DATE} {@link OdpsType#TIMESTAMP} {@link OdpsType#TIMESTAMP_NTZ} with timezone provided
     *
     * @param timezone timezone string
     * @return this
     */
    public OdpsRecordConverterBuilder timezone(String timezone) {
        this.config.timezone = ZoneId.of(timezone);
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse Odps Complex Data Type(Array/Struct/Map)
     * in json style.
     * <p>
     * nested data type convert rule:
     *  <li>OdpsType.TINYINT, OdpsType.SMALLINT, OdpsType.INT, OdpsType.BIGINT,
     *  OdpsType.FLOAT, OdpsType.DOUBLE, OdpsType.DECIMAL convert to json number.
     *  <li>OdpsType.CHAR,OdpsType.VARCHAR,OdpsType.STRING,OdpsType.DATE,OdpsType.DATETIME,OdpsType.TIMESTAMP,
     *  OdpsType.TIMESTAMP_NTZ, OdpsType.BINARY, OdpsType.JSON will convert to json string.
     *  <li>OdpsType.BOOLEAN convert to json boolean
     *  <li>OdpsType.ARRAY convert to json array.
     *  <li>OdpsType.MAP convert to json object, key of map convert to string
     *  <li>OdpsType.STRUCT convert to json object
     *
     * @return this
     */
    public OdpsRecordConverterBuilder complexFormatJson() {
        this.config.complexTypeFormat = COMPLEX_TYPE_FORMAT_JSON;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse Odps Complex Data Type(Array/Struct/Map)
     * in json style. Json number/boolean will not be used, all primitive type will convert to json string
     *
     * @return this
     */
    public OdpsRecordConverterBuilder complexFormatJsonStr() {
        this.config.complexTypeFormat = COMPLEX_TYPE_FORMAT_JSON_STR;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse Odps Complex Data Type(Array/Struct/Map)
     * in javaScript object style.
     *
     * @return this
     */
    public OdpsRecordConverterBuilder complexFormatHumanReadable() {
        this.config.complexTypeFormat = COMPLEX_TYPE_OUTPUT_FORMAT_HUMAN_READABLE;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#BINARY} in Quoted-Printable encoding
     *
     * @return this
     */
    public OdpsRecordConverterBuilder binaryFormatQuotedPrintable() {
        this.config.binaryFormat = ConverterConstant.BINARY_FORMAT_QUOTED_PRINTABLE;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#BINARY} in UTF8 encoding
     *
     * @return this
     */
    public OdpsRecordConverterBuilder binaryFormatUtf8() {
        this.config.binaryFormat = ConverterConstant.BINARY_FORMAT_UTF8;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#BINARY} in base64 encoding
     *
     * @return this
     */
    public OdpsRecordConverterBuilder binaryFormatBase64() {
        this.config.binaryFormat = ConverterConstant.BINARY_FORMAT_BASE64;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse {@link OdpsType#BINARY} in hex encoding
     *
     * @return this
     */
    public OdpsRecordConverterBuilder binaryFormatHex() {
        this.config.binaryFormat = ConverterConstant.BINARY_FORMAT_HEX;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse null object using provided format
     *
     * @return this
     */
    public OdpsRecordConverterBuilder nullFormat(String format) {
        this.config.nullFormat = format;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to parse null object using provided format
     *
     * @return this
     */
    public OdpsRecordConverterBuilder enableParseNull() {
        this.config.enableNullParse = true;
        return this;
    }

    /**
     * Configures OdpsRecordConverter not to parse null object using provided format
     *
     * @return this
     */
    public OdpsRecordConverterBuilder disableParseNull() {
        this.config.enableNullParse = false;
        return this;
    }

    /** Let OdpsRecordConverter no longer strictly check the incoming data type,
     * but perform type conversion operations as much as possible,
     * and then report an error when conversion cannot be performed.
     * <p>
     * This method replaces the useLegacyTimeType method, because when using this method,
     * the old type will be automatically converted to the new type.
     *
     * @return this
     */
    public OdpsRecordConverterBuilder setStrictMode(boolean strictMode) {
        this.config.strictMode = strictMode;
        return this;
    }

    /**
     * @deprecated since 0.50.0, will be removed in 0.51.0. use setStrictMode(false) instead.
     */
    @Deprecated
    public OdpsRecordConverterBuilder useLegacyTimeType() {
        setStrictMode(false);
        return this;
    }

    /**
     * provide an Odps Record Provider for {@link OdpsRecordConverter#parseRecord(String[], Column[])} to build new Record
     * using Column[]
     *
     * @param recordProvider - record provider
     * @return this
     */
    public OdpsRecordConverterBuilder registerRecordProvider(Function<Column[], Record> recordProvider) {
        this.recordProvider = recordProvider;
        return this;
    }

    /**
     * Configures OdpsRecordConverter to format/parse object using custom {@link OdpsObjectConverter}
     *
     * @param odpsType - type to format/parse
     * @param converter - custom converter
     * @return this
     */
    public OdpsRecordConverterBuilder register(OdpsType odpsType, OdpsObjectConverter converter) {
        this.formatterMap.put(odpsType, converter);
        return this;
    }

    // unsupported: VOID, INTERVAL_DAY_TIME, INTERVAL_YEAR_MONTH, UNKNOWN
    private static final OdpsType[] defaultSupportedTypes = {
            OdpsType.TINYINT,
            OdpsType.SMALLINT,
            OdpsType.INT,
            OdpsType.BIGINT,

            OdpsType.CHAR,
            OdpsType.VARCHAR,
            OdpsType.STRING,

            OdpsType.FLOAT,
            OdpsType.DOUBLE,
            OdpsType.DECIMAL,

            OdpsType.DATE,
            OdpsType.DATETIME,
            OdpsType.TIMESTAMP,
            OdpsType.TIMESTAMP_NTZ,

            OdpsType.BOOLEAN,
            OdpsType.BINARY,
            OdpsType.JSON,

            OdpsType.ARRAY,
            OdpsType.MAP,
            OdpsType.STRUCT
    };

    /**
     * Build an {@link OdpsRecordConverter} instance based on current configuration.
     * @return an instance of OdpsRecordConverter
     */
    public OdpsRecordConverter build() {

        for (OdpsType odpsType : defaultSupportedTypes) {
            if (!formatterMap.containsKey(odpsType)) {
                formatterMap.put(odpsType, ObjectConverterFactory.getFormatter(odpsType, config));
            }
        }

        if (recordProvider == null) {
            recordProvider = ArrayRecord::new;
        }

        return new OdpsRecordConverter(
                config,
                recordProvider,
                formatterMap);
    }


}
