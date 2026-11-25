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

import java.io.IOException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Helpers for the server's per-block commit message JSON:
 * {"BlockNumber":..,"AttemptNumber":..,"RecordNum":..,"WriterStats":{...}}.
 */
final class V2CommitMessages {

    private V2CommitMessages() {
    }

    static void check(String message, int blockNumber, int attemptNumber) throws IOException {
        JsonObject tree = parse(message);
        if (tree.has("BlockNumber") && tree.get("BlockNumber").getAsLong() != blockNumber) {
            throw new IOException("Unexpected block number, expected = " + blockNumber
                    + ", but " + tree.get("BlockNumber").getAsLong());
        }
        if (tree.has("AttemptNumber")
                && tree.get("AttemptNumber").getAsInt() != attemptNumber) {
            throw new IOException("Unexpected attempt number, expected = " + attemptNumber
                    + ", but " + tree.get("AttemptNumber").getAsInt());
        }
    }

    static long recordCount(String message, long fallback) {
        try {
            JsonObject tree = parse(message);
            if (tree.has("RecordNum")) {
                return tree.get("RecordNum").getAsLong();
            }
            if (tree.has("WriterStats") && tree.getAsJsonObject("WriterStats").has("RecordNum")) {
                return tree.getAsJsonObject("WriterStats").get("RecordNum").getAsLong();
            }
        } catch (Exception ignored) {
            // Fall back to the locally counted rows when the message lacks stats.
        }
        return fallback;
    }

    private static JsonObject parse(String message) {
        return new JsonParser().parse(message).getAsJsonObject();
    }
}
