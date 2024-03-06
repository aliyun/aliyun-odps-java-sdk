package com.aliyun.odps.data.converter;

import com.aliyun.odps.type.TypeInfo;

/**
 * Converts ODPS Java objects to and from String
 */
public interface OdpsObjectConverter {

    /**
     * {@link OdpsRecordConverter} invokes this method during format when it encounters an object of specified type
     * @param object - the object that needs to be formatted to string
     * @param typeInfo - the actual type of the object
     * @param converter - provide {@link OdpsRecordConverter#formatObject(Object, TypeInfo)} to format nested objects
     * @return a string representation of the given object
     */
    String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter);

    /**
     * {@link OdpsRecordConverter} invokes this method during parse when it encounters an object of specified type
     * @param str - the string that needs to be parsed to object
     * @param typeInfo - the actual type of the object
     * @param converter - provide {@link OdpsRecordConverter#parseObject(String, TypeInfo)} to parse nested objects
     * @return a parsed object of the specified type
     */
    Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter);

}
