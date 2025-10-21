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

package com.aliyun.odps.table.read;

import com.aliyun.odps.table.utils.Preconditions;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public interface TableSnapshotSpec extends Serializable {

    default Type getType() {
        return Type.UNSPECIFIED;
    }

    default Optional<Long> getVersion() {
        return Optional.empty();
    }

    default Optional<String> getTimestamp() {
        return Optional.empty();
    }

    enum Type {
        TIMESTAMP,
        VERSION,
        LATEST,
        UNSPECIFIED;

        @Override
        public String toString() {
            switch (this) {
                case TIMESTAMP:
                    return "timestamp";
                case VERSION:
                    return "version";
                case LATEST:
                    return "latest";
                case UNSPECIFIED:
                    return "unspecified";
                default:
                    throw new IllegalArgumentException("Unexpected invalid type");
            }
        }
    }

    class TableAsOfTimestamp implements TableSnapshotSpec {

        private final String timestamp;

        private TableAsOfTimestamp(String timestamp) {
            Preconditions.checkNotNull(timestamp, "timestamp");
            this.timestamp = timestamp;
        }

        @Override
        public Optional<String> getTimestamp() {
            return Optional.of(timestamp);
        }

        @Override
        public Type getType() {
            return Type.TIMESTAMP;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableAsOfTimestamp that = (TableAsOfTimestamp) o;
            return Objects.equals(timestamp, that.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp);
        }

        @Override
        public String toString() {
            return "TableAsOfTimestamp{" +
                    "timestamp='" + timestamp + '\'' +
                    '}';
        }

        public static TableAsOfTimestamp create(String timestamp) {
            return new TableAsOfTimestamp(timestamp);
        }
    }

    class TableAsOfVersion implements TableSnapshotSpec {

        private final Long version;

        private TableAsOfVersion(Long version) {
            Preconditions.checkNotNull(version, "version");
            this.version = version;
        }

        @Override
        public Optional<Long> getVersion() {
            return Optional.of(version);
        }

        @Override
        public Type getType() {
            return Type.VERSION;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableAsOfVersion that = (TableAsOfVersion) o;
            return Objects.equals(version, that.version);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version);
        }

        @Override
        public String toString() {
            return "TableAsOfVersion{" +
                    "version=" + version +
                    '}';
        }

        public static TableAsOfVersion create(Long version) {
            return new TableAsOfVersion(version);
        }
    }

    class TableLatest implements TableSnapshotSpec {

        private static final TableLatest INSTANCE = new TableLatest();

        private TableLatest() {
        }

        @Override
        public Type getType() {
            return Type.LATEST;
        }

        @Override
        public String toString() {
            return "TableLatest";
        }

        public static TableLatest create() {
            return INSTANCE;
        }
    }

    class TableVersionUnspecified implements TableSnapshotSpec {

        private static final TableVersionUnspecified INSTANCE = new TableVersionUnspecified();

        private TableVersionUnspecified() {
        }

        @Override
        public String toString() {
            return "TableVersionUnspecified";
        }

        public static TableVersionUnspecified create() {
            return INSTANCE;
        }
    }
}
