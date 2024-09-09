package com.aliyun.odps.data.converter;

class ConverterConstant {

    /**
     * decimal
     */
    static final String DECIMAL_OUTPUT_FORMAT_ZERO_PADDING = "zero_padding";
    static final String DECIMAL_OUTPUT_FORMAT_NORMAL = "non_zero_padding";
    static final String DECIMAL_OUTPUT_FORMAT_DEFAULT = DECIMAL_OUTPUT_FORMAT_NORMAL;
    static final String FLOATING_NUMBER_OUTPUT_FORMAT_SQL_COMPATIBLE = "sql_compatible";
    static final String FLOATING_NUMBER_OUTPUT_FORMAT_TO_STRING = "to_string";
    static final String FLOATING_NUMBER_OUTPUT_FORMAT_DEFAULT = FLOATING_NUMBER_OUTPUT_FORMAT_TO_STRING;

    /**
     * complex type: array / map / struct
     */
    // format in json
    static final String COMPLEX_TYPE_FORMAT_JSON = "json";

    // format in json, convert all type to string
    static final String COMPLEX_TYPE_FORMAT_JSON_STR = "json_str";

    // output format like javascript object (json without quotation)
    // same as SQL select human-readable output format
    // can not be parseFormat
    static final String COMPLEX_TYPE_OUTPUT_FORMAT_HUMAN_READABLE = "human_readable";
    static final String COMPLEX_TYPE_FORMAT_DEFAULT = COMPLEX_TYPE_FORMAT_JSON;

    /**
     * binary
     */
    static final String BINARY_FORMAT_UTF8 = "utf8";
    static final String BINARY_FORMAT_BASE64 = "base64";
    static final String BINARY_FORMAT_HEX = "hex";
    static final String BINARY_FORMAT_SQL_FORMAT = "sql";
    static final String BINARY_FORMAT_QUOTED_PRINTABLE = "quoted_printable";
    static final String BINARY_FORMAT_DEFAULT = BINARY_FORMAT_QUOTED_PRINTABLE;

    static final String NULL_OUTPUT_FORMAT_DEFAULT = "NULL";
}
