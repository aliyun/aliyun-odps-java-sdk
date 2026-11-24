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

package com.aliyun.odps.table.read.impl.v2;

import java.io.IOException;
import java.util.Optional;

import com.aliyun.odps.table.SessionStatus;
import com.google.gson.JsonObject;

/**
 * Raw plus parsed view of one Storage API v2 read-session response.
 *
 * <p>The raw JSON is what {@code TableBatchReadSessionV2Impl#toJson()} persists, so all
 * session metadata survives process restarts without a server round-trip; parsed accessors
 * are computed lazily from the same tree.
 */
class V2SessionResult {

    private final String rawJson;

    private final JsonObject tree;

    private volatile SessionStatus status;

    V2SessionResult(JsonObject tree) {
        this.tree = tree;
        this.rawJson = tree.toString();
    }

    String rawJson() {
        return rawJson;
    }

    JsonObject tree() {
        return tree;
    }

    String sessionId() {
        return tree.has("SessionId") ? tree.get("SessionId").getAsString() : null;
    }

    SessionStatus sessionStatus() {
        if (status == null) {
            status = tree.has("SessionStatus")
                    ? SessionStatus.valueOf(tree.get("SessionStatus").getAsString().toUpperCase())
                    : SessionStatus.UNKNOWN;
        }
        return status;
    }

    String message() {
        return tree.has("Message") ? tree.get("Message").getAsString() : null;
    }

    long expirationTime() {
        return tree.has("ExpirationTime") ? tree.get("ExpirationTime").getAsLong() : 0L;
    }

    boolean extendedArrowIPCEnabled() {
        return tree.has("ExtendedArrowIPCEnabled")
                && !tree.get("ExtendedArrowIPCEnabled").isJsonNull()
                && tree.get("ExtendedArrowIPCEnabled").getAsBoolean();
    }

    Optional<String> optionalString(String field) {
        return tree.has(field) && !tree.get(field).isJsonNull()
                ? Optional.of(tree.get(field).getAsString()) : Optional.empty();
    }
}
