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

public class StreamSessionBase extends SessionBase {
    protected String schemaVersion;

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

                if (tree.has("schema_version")) {
                    // schemaVersion
                    schemaVersion = tree.get("schema_version").getAsString();
                }
            }

            if (tree.has("quota_name")) {
                quotaName = tree.get("quota_name").getAsString();
            }

            if (tree.has("slots") && tree.has("status")) {
                String status = tree.get("status").getAsString();
                if (status.equals("init")) {
                    throw new TunnelException(requestId, "Session is initiating. Session name: " + id);
                }
                // slots
                return Util.parseSlots(tree.getAsJsonArray("slots"));
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
}
