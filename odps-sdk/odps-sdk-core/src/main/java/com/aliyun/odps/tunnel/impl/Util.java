package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.TunnelConstants;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.aliyun.odps.tunnel.TunnelConstants.TUNNEL_DATE_TRANSFORM_VERSION;

public class Util {
    public static HashMap<String, String> getCommonHeader() {
        HashMap<String, String> headers = new HashMap<>();

        headers.put(Headers.CONTENT_LENGTH, String.valueOf(0));
        headers.put(HttpHeaders.HEADER_ODPS_DATE_TRANSFORM, TUNNEL_DATE_TRANSFORM_VERSION);
        headers.put(HttpHeaders.HEADER_ODPS_TUNNEL_VERSION, String.valueOf(TunnelConstants.VERSION));
        return headers;
    }

    public static RestClient newRestClient(Configuration conf, String projectName) throws TunnelException {
        return newRestClient(conf, conf.getEndpoint(projectName));
    }

    public static RestClient newRestClient(Configuration conf, URI endpoint) {

        RestClient odpsServiceClient = conf.getOdps().clone().getRestClient();

        odpsServiceClient.setReadTimeout(conf.getSocketTimeout());
        odpsServiceClient.setConnectTimeout(conf.getSocketConnectTimeout());
        odpsServiceClient.setEndpoint(endpoint.toString());

        return odpsServiceClient;
    }

    public static List<Slot> parseSlots(JsonArray slotElements) throws TunnelException {
        List<Slot> slots = new ArrayList<>();

        for (JsonElement slot : slotElements) {

            if (!slot.isJsonArray()) {
                throw new TunnelException("Invalid slot routes");
            }

            JsonArray slotInfo = slot.getAsJsonArray();
            if (slotInfo.size() != 2) {
                throw new TunnelException("Invalid slot routes");
            }
            slots.add(new Slot(slotInfo.get(0).getAsString(), slotInfo.get(1).getAsString()));
        }

        return slots;
    }
}
