package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.Odps;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TunnelException;

public class ConfigurationImpl extends Configuration {
    public ConfigurationImpl(Odps odps) {
        super(odps);
    }

    public RestClient newRestClient(String projectName) throws TunnelException {

        RestClient odpsServiceClient = odps.clone().getRestClient();

        odpsServiceClient.setReadTimeout(getSocketTimeout());
        odpsServiceClient.setConnectTimeout(getSocketConnectTimeout());
        odpsServiceClient.setEndpoint(getEndpoint(projectName).toString());

        return odpsServiceClient;
    }
}
