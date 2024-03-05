package com.aliyun.odps.utils;

public class ExceptionUtils {

    public static void checkStringArgumentNotNull(String arg, String value) {
        if (StringUtils.isNullOrEmpty(value)) {
            throw new IllegalArgumentException("Argument '" + arg + "' cannot be null or empty");
        }
    }

}
