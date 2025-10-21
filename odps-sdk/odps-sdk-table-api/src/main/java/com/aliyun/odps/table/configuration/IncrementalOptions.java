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

import com.aliyun.odps.table.read.TableSnapshotSpec.TableAsOfTimestamp;
import com.aliyun.odps.table.read.TableSnapshotSpec.TableAsOfVersion;
import com.aliyun.odps.table.read.TableSnapshotSpec;
import com.aliyun.odps.table.utils.Preconditions;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

public class IncrementalOptions implements Serializable {

    public static final IncrementalOptions NO_INCREMENTAL_OPTION =
            new IncrementalOptions();

    private TableAsOfVersion startVersion;

    private TableAsOfVersion endVersion;

    private TableAsOfTimestamp startTimestamp;

    private TableAsOfTimestamp endTimestamp;

    private IncrementalMode mode;

    private TableSnapshotSpec.Type type;

    private IncrementalOptions() {
        this.mode = IncrementalMode.APPEND;
        this.type = TableSnapshotSpec.Type.UNSPECIFIED;
    }

    public Optional<TableAsOfVersion> getStartVersion() {
        return Optional.ofNullable(startVersion);
    }

    public Optional<TableAsOfVersion> getEndVersion() {
        return Optional.ofNullable(endVersion);
    }

    public Optional<TableAsOfTimestamp> getStartTimestamp() {
        return Optional.ofNullable(startTimestamp);
    }

    public Optional<TableAsOfTimestamp> getEndTimestamp() {
        return Optional.ofNullable(endTimestamp);
    }

    public IncrementalMode getIncrementalMode() {
        return mode;
    }

    public TableSnapshotSpec.Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IncrementalOptions)) return false;
        IncrementalOptions that = (IncrementalOptions) o;
        return Objects.equals(startVersion, that.startVersion) &&
                Objects.equals(endVersion, that.endVersion) &&
                Objects.equals(startTimestamp, that.startTimestamp) &&
                Objects.equals(endTimestamp, that.endTimestamp) &&
                mode == that.mode &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startVersion, endVersion, startTimestamp, endTimestamp, mode, type);
    }

    public static IncrementalOptions.Builder newBuilder() {
        return new IncrementalOptions.Builder();
    }

    public enum IncrementalMode {
        APPEND,
        CDC;

        @Override
        public String toString() {
            switch (this) {
                case APPEND:
                    return "append";
                case CDC:
                    return "cdc";
                default:
                    throw new IllegalArgumentException("Unexpected invalid mode");
            }
        }

        public static IncrementalMode fromString(String mode) {
            if (mode == null) {
                throw new IllegalArgumentException("Mode cannot be null");
            }

            switch (mode.toLowerCase()) {
                case "append":
                    return APPEND;
                case "cdc":
                    return CDC;
                default:
                    throw new IllegalArgumentException("Unexpected mode: " + mode);
            }
        }
    }

    public static class Builder {

        private final IncrementalOptions options = new IncrementalOptions();

        public IncrementalOptions.Builder startVersion(Long startVersion) {
            Preconditions.checkArgument(options.type == TableSnapshotSpec.Type.VERSION
                            || options.type == TableSnapshotSpec.Type.UNSPECIFIED,
                    "Incremental options type is " + options.type);
            this.options.startVersion = TableAsOfVersion.create(startVersion);
            this.options.type = TableSnapshotSpec.Type.VERSION;
            return this;
        }

        public IncrementalOptions.Builder endVersion(Long startVersion) {
            Preconditions.checkArgument(options.type == TableSnapshotSpec.Type.VERSION
                            || options.type == TableSnapshotSpec.Type.UNSPECIFIED,
                    "Incremental options type is " + options.type);
            this.options.endVersion = TableAsOfVersion.create(startVersion);
            this.options.type = TableSnapshotSpec.Type.VERSION;
            return this;
        }

        public IncrementalOptions.Builder startTimeStamp(String timestamp) {
            Preconditions.checkArgument(options.type == TableSnapshotSpec.Type.TIMESTAMP
                            || options.type == TableSnapshotSpec.Type.UNSPECIFIED,
                    "Incremental options type is " + options.type);
            this.options.startTimestamp = TableAsOfTimestamp.create(timestamp);
            this.options.type = TableSnapshotSpec.Type.TIMESTAMP;
            return this;
        }

        public IncrementalOptions.Builder endTimeStamp(String timestamp) {
            Preconditions.checkArgument(options.type == TableSnapshotSpec.Type.TIMESTAMP
                            || options.type == TableSnapshotSpec.Type.UNSPECIFIED,
                    "Incremental options type is " + options.type);
            this.options.endTimestamp = TableAsOfTimestamp.create(timestamp);
            this.options.type = TableSnapshotSpec.Type.TIMESTAMP;
            return this;
        }

        public IncrementalOptions.Builder withIncrementalMode(IncrementalMode mode) {
            this.options.mode = mode;
            return this;
        }

        public IncrementalOptions build() {
            Preconditions.checkArgument(options.getStartVersion().isPresent()
                            || options.getStartTimestamp().isPresent(),
                    "Must set either startVersion or startTimestamp");
            return this.options;
        }
    }

}
