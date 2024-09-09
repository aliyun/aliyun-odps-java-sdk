package com.aliyun.odps.data.converter;

import static com.aliyun.odps.data.OdpsTypeTransformer.ODPS_TYPE_MAPPER;
import static com.aliyun.odps.data.OdpsTypeTransformer.ODPS_TYPE_MAPPER_V2;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.OdpsTypeTransformer;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.type.TypeInfo;

/**
 * This class provides methods to convert between ODPS objects and strings
 * <p>
 * You can create an OdpsRecordConverter by invoking {@link OdpsRecordConverter#defaultConverter()}
 * if the default configuration is all you need.
 * You can also use {@link OdpsRecordConverterBuilder} to build an OdpsRecordConverter instance with various configuration options
 * such as Datetime pattern, Decimal padding trailing zeros, custom {@link OdpsObjectConverter}.
 * <p>
 * Here is an example of how OdpsRecordConverter is used for a simple Class
 * <pre>
 * {@code
 *     OdpsRecordConverter converter = OdpsRecordConverter.defaultConverter();
 *
 *     // format/parse object
 *     String formattedDate = converter.formatObject(LocalDate.now(), TypeInfoFactory.DATE);
 *     LocalDate localDate = (LocalDate) converter.parseObject("2022-10-10", TypeInfoFactory.DATE);
 *
 *     // format/parse record
 *     Column[] cols = new Column[]{new Column("i", OdpsType.INT)}
 *     ArrayRecord record = new ArrayRecord(cols);
 *     record.set(0, 100000);
 *     String[] formattedArray = converter.formatRecord(record);
 *     Record parsedRecord = converter.parseRecord(new String[]{"100"}, cols);
 * }
 * </pre>
 */
public class OdpsRecordConverter {

    // not working for complex type
    final String nullFormat;
    final boolean enableNullParse;
    final boolean strictMode;
    Function<Column[], Record> recordProvider;
    Map<OdpsType, OdpsObjectConverter> objectConverterMap;

    OdpsRecordConverter(OdpsRecordConverterBuilder.Config config,
                        Function<Column[], Record> recordProvider,
                        Map<OdpsType, OdpsObjectConverter> objectConverterMap) {
        this.nullFormat = config.nullFormat;
        this.enableNullParse = config.enableNullParse;
        this.strictMode = config.strictMode;
        this.recordProvider = recordProvider;
        this.objectConverterMap = objectConverterMap;
    }

    /**
     * Create an OdpsRecordConverterBuilder instance
     * that can be used to build OdpsRecordConverter with various configuration settings.
     *
     * @return an OdpsRecordConverterBuilder
     */
    public static OdpsRecordConverterBuilder builder() {
        return new OdpsRecordConverterBuilder();
    }

    /**
     * Get an OdpsRecordConverter instance with default format/parse configuration
     *
     * @return an OdpsRecordConverter
     */
    public static OdpsRecordConverter defaultConverter() {
        return builder().build();
    }

    /**
     * Format an ODPS Java Object to String
     * @param object Odps Java object
     * @param typeInfo object type
     * @return formatted result string
     */
    public String formatObject(Object object, TypeInfo typeInfo) {
        if (null == object) {
            return nullFormat;
        }
        if (strictMode) {
            Class
                expectClass =
                OdpsTypeTransformer.odpsTypeToJavaType(ODPS_TYPE_MAPPER_V2, typeInfo.getOdpsType());
            try {
                if (typeInfo.getOdpsType() == OdpsType.STRING && object instanceof byte[]) {
                    object = new String((byte[]) object, StandardCharsets.UTF_8);
                } else {
                    object = expectClass.cast(object);
                }
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Cannot format " + object
                                                   + "(" + object.getClass() + ") to ODPS type: "
                                                   + typeInfo.getOdpsType()
                                                   + ", expect java class: "
                                                   + expectClass.getName(), e);
            }
        }
        return objectConverterMap.get(typeInfo.getOdpsType())
                .format(object, typeInfo, this);
    }

    /**
     * Convert an Odps {@link Record} to String Array,
     * elements in the array corresponds to the string representation in Record
     * @param record Odps Record
     * @return a string representation array String[] of the given Record
     */
    public String[] formatRecord(Record record) {
        Column[] columns = record.getColumns();
        String[] result = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            TypeInfo typeInfo = columns[i].getTypeInfo();
            result[i] = formatObject(record.get(i), typeInfo);
        }
        return result;
    }

    /**
     * Get an iterable of formatted string array by iterable of odps {@link Record}
     * @param resultSet Iterable<Record>
     * @return an Iterable of String[]
     */
    public Iterable<String[]> formatResultSet(Iterable<Record> resultSet) {
        return new OdpsFormatResultSet(resultSet, this);
    }

    /**
     * Parse a string to Odps Java Object
     * @param str - the string that needs to be parsed to object
     * @param typeInfo - the type of target Odps Object
     * @return a parsed object of the specified type
     */
    public Object parseObject(String str, TypeInfo typeInfo) {
        Objects.requireNonNull(str);
        if (enableNullParse && nullFormat.equals(str)) {
            return null;
        }
        return objectConverterMap.get(typeInfo.getOdpsType()).parse(str, typeInfo, this);
    }

    /**
     * Parse a String Array to Odps {@link Record}
     * @param stringArray input String array
     * @param columns column info
     * @return an odps Record
     */
    public Record parseRecord(String[] stringArray, Column[] columns) {
        Record record = recordProvider.apply(columns);
        for (int i = 0; i < columns.length; i++) {
            record.set(i, parseObject(stringArray[i], columns[i].getTypeInfo()));
        }
        return record;
    }

    /**
     * Get an iterable of Record by iterable of string array
     * @param recordSet string array iterable
     * @param columns columns info
     * @return an Iterable of Record
     */
    public Iterable<Record> parseResultSet(Iterable<String[]> recordSet, Column[] columns) {
        return new OdpsParseResultSet(recordSet, columns, this);
    }


    static class OdpsFormatResultSet implements Iterable<String[]> {

        private final Iterator<Record> sourceIterator;
        private final OdpsRecordConverter converter;

        OdpsFormatResultSet(Iterable<Record> sourceIterable,
                            OdpsRecordConverter converter) {
            this.sourceIterator = sourceIterable.iterator();
            this.converter = converter;
        }

        @Override
        public Iterator<String[]> iterator() {
            return new OdpsFormatResultSet.Itr();
        }

        private class Itr implements Iterator<String[]> {

            @Override
            public boolean hasNext() {
                return sourceIterator.hasNext();
            }

            @Override
            public String[] next() {
                return converter.formatRecord(sourceIterator.next());
            }
        }


    }

    static class OdpsParseResultSet implements Iterable<Record> {

        private final Iterator<String[]> sourceIterator;
        private final Column[] columns;
        private final OdpsRecordConverter parser;

        OdpsParseResultSet(Iterable<String[]> sourceIterable,
                           Column[] columns,
                           OdpsRecordConverter context) {
            this.sourceIterator = sourceIterable.iterator();
            this.columns = columns;
            this.parser = context;
        }

        @Override
        public Iterator<Record> iterator() {
            return new OdpsParseResultSet.Itr();
        }

        private class Itr implements Iterator<Record> {

            @Override
            public boolean hasNext() {
                return sourceIterator.hasNext();
            }

            @Override
            public Record next() {
                return parser.parseRecord(sourceIterator.next(), columns);
            }
        }


    }

}
