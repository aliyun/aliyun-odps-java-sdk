package com.aliyun.odps.options;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsOptions {

    /**
     * true for S52, null after S53
     */
    private Boolean useLegacyLogview = true;

    private Boolean skipCheckIfEpv2;

    public void setUseLegacyLogview(Boolean useLegacyLogview) {
        this.useLegacyLogview = useLegacyLogview;
    }

    public Boolean isUseLegacyLogview() {
        return useLegacyLogview;
    }

    public void setSkipCheckIfEpv2(Boolean skipCheckIfEpv2) {
        this.skipCheckIfEpv2 = skipCheckIfEpv2;
    }

    public Boolean isSkipCheckIfEpv2() {
        return skipCheckIfEpv2;
    }
}