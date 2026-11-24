/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.table.write.impl.v2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.Column;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.distribution.Distribution;
import com.aliyun.odps.table.distribution.Distributions;
import com.aliyun.odps.table.order.NullOrdering;
import com.aliyun.odps.table.order.SortDirection;
import com.aliyun.odps.table.order.SortOrder;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.table.utils.SessionUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Parses Storage API v2 batch-compatible write-session response JSON into the table-api
 * write session model. Field names follow the server-side write session response schema.
 */
final class V2WriteResponses {

    private V2WriteResponses() {
    }

    static DataSchema parseDataSchema(JsonObject dataSchema) {
        List<Column> schemaColumns = new ArrayList<>();
        List<String> partitionKeys = new ArrayList<>();
        if (dataSchema.has("DataColumns")) {
            dataSchema.get("DataColumns").getAsJsonArray().forEach(column ->
                    schemaColumns.add(SchemaUtils.parseColumn(column.getAsJsonObject())));
        }
        if (dataSchema.has("PartitionColumns")) {
            dataSchema.get("PartitionColumns").getAsJsonArray().forEach(column -> {
                Column partitionCol = SchemaUtils.parseColumn(column.getAsJsonObject());
                schemaColumns.add(partitionCol);
                partitionKeys.add(partitionCol.getName());
            });
        }
        return DataSchema.newBuilder().columns(schemaColumns)
                .partitionBy(partitionKeys).build();
    }

    static Set<DataFormat> parseSupportedDataFormats(JsonObject tree) {
        if (!tree.has("SupportedDataFormat")) {
            return null;
        }
        Set<DataFormat> formats = new HashSet<>();
        JsonArray array = tree.get("SupportedDataFormat").getAsJsonArray();
        array.forEach(format -> formats.add(SessionUtils.parseDataFormat(format.getAsJsonObject())));
        return formats;
    }

    static SortOrder[] parseRequiredOrdering(JsonObject tree) {
        if (!tree.has("RequiredOrdering")) {
            return new SortOrder[]{};
        }
        List<SortOrder> sortOrders = new ArrayList<>();
        tree.get("RequiredOrdering").getAsJsonArray()
                .forEach(order -> sortOrders.add(parseOrder(order.getAsJsonObject())));
        return sortOrders.toArray(new SortOrder[0]);
    }

    static Distribution parseRequiredDistribution(JsonObject tree) {
        if (!tree.has("RequiredDistribution")) {
            return Distributions.unspecified();
        }
        JsonObject distribution = tree.getAsJsonObject("RequiredDistribution");
        Distribution.Type type = Distribution.Type.UNSPECIFIED;
        if (distribution.has("Type")) {
            type = Distribution.Type.valueOf(distribution.get("Type").getAsString().toUpperCase());
        }
        if (type.equals(Distribution.Type.UNSPECIFIED)) {
            return Distributions.unspecified();
        }
        List<String> clusterKeys = new ArrayList<>();
        if (distribution.has("ClusterKeys")) {
            distribution.get("ClusterKeys").getAsJsonArray()
                    .forEach(key -> clusterKeys.add(key.getAsString()));
        }
        int bucketsNumber = -1;
        if (distribution.has("BucketsNumber")) {
            bucketsNumber = distribution.get("BucketsNumber").getAsInt();
        }
        return Distributions.clustered(clusterKeys, type, bucketsNumber);
    }

    private static SortOrder parseOrder(JsonObject order) {
        Preconditions.checkArgument(order.has("Name"), "Sort order must has name");
        String name = order.get("Name").getAsString();
        SortDirection sortDirection = order.has("SortDirection")
                ? SortDirection.valueOf(order.get("SortDirection").getAsString().toUpperCase())
                : SortDirection.NONE;
        NullOrdering nullOrdering = order.has("NullOrdering")
                ? NullOrdering.valueOf(order.get("NullOrdering").getAsString().toUpperCase())
                : NullOrdering.ANY;
        return new SortOrder(name, sortDirection, nullOrdering);
    }
}
