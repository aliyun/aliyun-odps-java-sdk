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

package com.aliyun.odps.table;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Data format for table reader/writer.
 */
public class DataFormat implements Serializable {

    private Type type;
    private Version version;

    public DataFormat() {
        this(Type.UNKNOWN, Version.UNKNOWN);
    }

    public DataFormat(Type type, Version version) {
        this.type = type;
        this.version = version;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public Version getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataFormat that = (DataFormat) o;
        return Objects.equals(type, that.type)
                && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, version);
    }

    public enum Type {
        ARROW("ARROW"),
        RECORD("RECORD"),
        UNKNOWN("UNKNOWN");

        private final String typeStr;

        Type(String typeStr) {
            this.typeStr = typeStr;
        }

        @Override
        public String toString() {
            return typeStr;
        }

        private static final Map<String, Type> TYPE_MAP =
                Arrays.stream(values())
                        .collect(Collectors.toMap(v -> v.typeStr, Function.identity()));

        public static Optional<Type> byTypeName(String type) {
            return Optional.ofNullable(TYPE_MAP.get(type));
        }
    }

    public enum Version {

        V5("V5"),
        UNKNOWN("UNKNOWN");

        private final String versionStr;

        Version(String versionStr) {
            this.versionStr = versionStr;
        }

        @Override
        public String toString() {
            return versionStr;
        }

        private static final Map<String, Version> CODE_MAP =
                Arrays.stream(values())
                        .collect(Collectors.toMap(v -> v.versionStr, Function.identity()));

        public static Optional<Version> byCode(String code) {
            return Optional.ofNullable(CODE_MAP.get(code));
        }
    }

}