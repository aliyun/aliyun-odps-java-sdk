package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.Odps;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TunnelException;

/**
 * use Configuration instead of ConfigurationImpl
 * <p>
 * Have not deleted this class is for interface compatibility
 */
@Deprecated
public class ConfigurationImpl extends Configuration {
    public ConfigurationImpl(Odps odps) {
        super(odps);
    }
}
