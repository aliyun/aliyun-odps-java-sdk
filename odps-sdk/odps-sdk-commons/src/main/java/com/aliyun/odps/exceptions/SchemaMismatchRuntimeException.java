package com.aliyun.odps.exceptions;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 * <p>
 * client side schema mismatch exception.
 * only throw at when input value of record is not match with table schema.
 */
public class SchemaMismatchRuntimeException extends IllegalArgumentException {

    public SchemaMismatchRuntimeException(String errorMsg) {
        super(errorMsg);
    }

    public SchemaMismatchRuntimeException(String errorMsg, Exception e) {
        super(errorMsg, e);
    }
}
