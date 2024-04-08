package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.Odps;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;

public class ConfigurationImpl extends Configuration {
    public ConfigurationImpl(Odps odps) {
        super(odps);
    }

    public RestClient newRestClient(String projectName) throws TunnelException {

        RestClient odpsServiceClient = odps.clone().getRestClient();

        odpsServiceClient.setReadTimeout(getSocketTimeout());
        odpsServiceClient.setConnectTimeout(getSocketConnectTimeout());

        if (StringUtils.isNullOrEmpty(odps.getTunnelEndpoint())) {
            odpsServiceClient.setEndpoint(getEndpoint(projectName).toString());
        } else {
            odpsServiceClient.setEndpoint(odps.getTunnelEndpoint());
        }
        return odpsServiceClient;
    }
}
