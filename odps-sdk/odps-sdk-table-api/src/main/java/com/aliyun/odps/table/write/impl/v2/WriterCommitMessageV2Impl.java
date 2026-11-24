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

import com.aliyun.odps.storage.write.BlockWriteResult;
import com.aliyun.odps.table.write.WriterCommitMessage;

/**
 * Commit message produced by {@link ArrowWriterV2Impl}: wraps the SDK's block write result
 * so {@code commit(WriterCommitMessage[])} can delegate validation to the v2 client.
 */
public class WriterCommitMessageV2Impl implements WriterCommitMessage {

    private static final long serialVersionUID = 1L;

    private final BlockWriteResult blockWriteResult;

    public WriterCommitMessageV2Impl(BlockWriteResult blockWriteResult) {
        this.blockWriteResult = blockWriteResult;
    }

    public BlockWriteResult blockWriteResult() {
        return blockWriteResult;
    }

    public int getBlockNumber() {
        return blockWriteResult.getBlockNumber();
    }

    public long getRecordCount() {
        return blockWriteResult.getRecordCount();
    }
}
