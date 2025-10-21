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

import com.aliyun.odps.table.read.split.InputSplitWithBucket;
import com.aliyun.odps.table.read.split.InputSplitWithIndex;
import com.google.common.base.Preconditions;

public class BucketInputSplit implements InputSplitWithIndex, InputSplitWithBucket {

    private static final long serialVersionUID = 1L;

    private final String sessionId;

    private final int splitIndex;

    private final int bucketId;

    public BucketInputSplit(
            String sessionId,
            int index,
            int bucketId) {
        Preconditions.checkArgument(index >= 0, "index must be >= 0");
        this.sessionId = Preconditions.checkNotNull(sessionId);
        this.splitIndex = index;
        this.bucketId = bucketId;
    }

    @Override
    public int getBucketId() {
        return bucketId;
    }

    @Override
    public int getSplitIndex() {
        return splitIndex;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof BucketInputSplit) {
            BucketInputSplit other = (BucketInputSplit) obj;
            return other.splitIndex == this.splitIndex
                    && other.bucketId == this.bucketId
                    && other.sessionId.equals(this.sessionId);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return String.format("BucketInputSplit: SessionId=%s SplitIndex=%s BucketId=%s",
                sessionId, splitIndex, bucketId);
    }
}
