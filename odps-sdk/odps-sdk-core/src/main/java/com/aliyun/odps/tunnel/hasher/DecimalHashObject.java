package com.aliyun.odps.tunnel.hasher;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */

import java.math.BigDecimal;

public class DecimalHashObject {

    private BigDecimal val;
    private int precision;
    private int scale;

    public DecimalHashObject(BigDecimal val, int precision, int scale) {
        this.val = val;
        this.precision = precision;
        this.scale = scale;
    }

    public BigDecimal val() {
        return this.val;
    }

    public int precision() {
        return this.precision;
    }

    public int scale() {
        return this.scale;
    }
}
