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

package com.aliyun.odps.table.configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum CompressionCodec implements Serializable {
    NO_COMPRESSION(""),
    ZSTD("ZSTD"),
    LZ4_FRAME("LZ4_FRAME");
    private final String codecName;

    CompressionCodec(String codecName) {
        this.codecName = codecName;
    }

    @Override
    public String toString() {
        return codecName;
    }

    private static final Map<String, CompressionCodec> CODEC_MAP =
            Arrays.stream(values())
                    .collect(Collectors.toMap(v -> v.codecName, Function.identity()));

    public static Optional<CompressionCodec> byName(String name) {
        return Optional.ofNullable(CODEC_MAP.get(name.toUpperCase()));
    }
}