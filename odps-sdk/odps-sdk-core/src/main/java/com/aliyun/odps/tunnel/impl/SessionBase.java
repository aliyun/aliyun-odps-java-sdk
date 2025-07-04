package com.aliyun.odps.tunnel.impl;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRetryHandler;
import com.aliyun.odps.utils.StringUtils;

public abstract class SessionBase {
    protected String id;
    protected Configuration config;
    protected String projectName;
    protected String schemaName;
    protected String tableName;
    protected String partitionSpec;
    protected String quotaName;
    protected RestClient httpClient;
    protected TableSchema schema = new TableSchema();
    protected TunnelRetryHandler tunnelRetryHandler = new TunnelRetryHandler();

    public class HttpResult {
        public String requestId;
        public Integer status;
        public String body;
        HttpResult (String requestId, Integer status, String body) {
            this.requestId = requestId;
            this.status = status;
            this.body = body;
        }
    }

    protected abstract String getResource();

    protected SessionBase.HttpResult httpRequestWithNoRetry(HashMap<String, String> headers,
                                                            Map<String, String> params,
                                                            String method, String action)
        throws TunnelException {
        String requestId = null;
        Connection conn = null;
        try {
            conn = httpClient.connect(getResource(), method, params, headers);
            Response resp = conn.getResponse();
            requestId = resp.getHeader(HEADER_ODPS_REQUEST_ID);

            if (resp.isOK()) {
                return new SessionBase.HttpResult(requestId, resp.getStatus(),
                                                  IOUtils.readStreamAsString(
                                                      conn.getInputStream()));
            } else {
                throw new TunnelException(requestId, conn.getInputStream(),
                                          resp.getStatus());
            }
        } catch (IOException e) {
            throw new TunnelException(requestId,
                                      "Failed to " + action + " with tunnel endpoint "
                                      + httpClient.getEndpoint(), e);
        } catch (TunnelException e) {
            // Do not delete here! TunnelException extends from OdpsException.
            throw e;
        } catch (OdpsException e) {
            throw new TunnelException(requestId, e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.disconnect();
                } catch (IOException e) {
                }
            }
        }
    }

    protected SessionBase.HttpResult httpRequest(HashMap<String, String> headers,
                                                 Map<String, String> params, String method,
                                                 String action) throws TunnelException {
        try {
           return tunnelRetryHandler.executeWithRetry(
                () -> httpRequestWithNoRetry(headers, params, method, action));
        } catch (TunnelException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new TunnelException(e.getMessage(), e);
        }
    }

    protected HashMap<String, String> getCommonHeaders() {
        HashMap<String, String> commonHeaders = Util.getCommonHeader();
        List<String> tags = config.getTags();
        if (tags != null) {
            commonHeaders.put(HttpHeaders.HEADER_ODPS_TUNNEL_TAGS, String.join(",", tags));
        }
        return commonHeaders;
    }

    protected HashMap<String, String> getCommonParams() {
        HashMap<String, String> params = new HashMap<String, String>();

        if (this.partitionSpec != null && this.partitionSpec.length() > 0) {
            params.put(TunnelConstants.RES_PARTITION, partitionSpec);
        }

        if (!StringUtils.isNullOrEmpty(config.getQuotaName())) {
            params.put(TunnelConstants.PARAM_QUOTA_NAME, config.getQuotaName());
        }

        return params;
    }

    public TunnelRetryHandler getTunnelRetryHandler() {
        return tunnelRetryHandler;
    }
}
