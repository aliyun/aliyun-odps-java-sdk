package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.*;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aliyun.odps.tunnel.HttpHeaders.HEADER_ODPS_REQUEST_ID;

public class StreamSessionBase {
    protected String id;
    protected ConfigurationImpl config;
    protected TableSchema schema = new TableSchema();
    protected String projectName;
    protected String schemaName;
    protected String tableName;
    protected String partitionSpec;
    protected RestClient httpClient;


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

    protected HttpResult httpRequest(HashMap<String, String> headers, Map<String, String> params, String method, String action) throws TunnelException {
        String requestId = null;
        Connection conn = null;
        try {
            conn = httpClient.connect(getResource(), method, params, headers);
            Response resp = conn.getResponse();
            requestId = resp.getHeader(HEADER_ODPS_REQUEST_ID);

            if (resp.isOK()) {
                return new HttpResult(requestId, resp.getStatus(), IOUtils.readStreamAsString(conn.getInputStream()));
            } else {
                throw new TunnelException(requestId, conn.getInputStream(), resp.getStatus());
            }
        } catch (IOException e) {
            throw new TunnelException(requestId, "Failed to " + action + " with tunnel endpoint "
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

    protected List<Slot> loadFromJson(String requestId, JsonObject tree, boolean reload) throws TunnelException {
        try {
            if (!reload) {
                if (tree.has("session_name") && tree.has("schema")) {
                    // session id
                    id = tree.get("session_name").getAsString();
                    // schema
                    JsonObject tunnelTableSchema = tree.get("schema").getAsJsonObject();
                    schema = new TunnelTableSchema(tunnelTableSchema);
                } else {
                    throw new TunnelException(requestId, "Incomplete session info: '" + tree.toString() + "'");
                }
            }

            if (tree.has("slots") && tree.has("status")) {
                String status = tree.get("status").getAsString();
                if (status.equals("init")) {
                    throw new TunnelException(requestId, "Session is initiating. Session name: " + id);
                }
                // slots
                return Util.parseSlots(tree.getAsJsonArray("slots"), reload);
            } else {
                throw new TunnelException(requestId, "Incomplete session info: '" + tree.toString() + "'");
            }

        } catch (TunnelException e) {
            throw e;
        } catch (Exception e) {
            throw new TunnelException(requestId, "Invalid json content: '" + tree.toString() + "'", e);
        }
    }

    protected String getResource() {
        return config.getResource(projectName, schemaName, tableName)+ "/" + TunnelConstants.STREAMS;
    }

    protected HashMap<String, String> getCommonHeaders() {
        return Util.getCommonHeader();
    }

    protected HashMap<String, String> getCommonParams() {
        HashMap<String, String> params = new HashMap<String, String>();

        if (this.partitionSpec != null && this.partitionSpec.length() > 0) {
            params.put(TunnelConstants.RES_PARTITION, partitionSpec);
        }
        return params;
    }
}
