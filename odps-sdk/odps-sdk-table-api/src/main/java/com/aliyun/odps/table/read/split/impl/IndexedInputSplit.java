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

package com.aliyun.odps.table.read.split.impl;

import com.aliyun.odps.table.read.split.InputSplitWithIndex;
import com.google.common.base.Preconditions;

import java.util.Objects;

public class IndexedInputSplit implements InputSplitWithIndex {

    private static final long serialVersionUID = 1L;

    private final String sessionId;

    /**
     * The index of this split.
     */
    private final int splitIndex;

    public IndexedInputSplit(
            String sessionId,
            int index) {
        Preconditions.checkArgument(index >= 0, "index must be >= 0");
        this.sessionId = Preconditions.checkNotNull(sessionId);
        this.splitIndex = index;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public int getSplitIndex() {
        return splitIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.splitIndex);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof IndexedInputSplit) {
            IndexedInputSplit other = (IndexedInputSplit) obj;
            return other.splitIndex == this.splitIndex
                    && other.sessionId.equals(this.sessionId);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("IndexedInputSplit: SessionId=%s SplitIndex=%s",
                sessionId, splitIndex);
    }
}
