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

package com.aliyun.odps.table.write;

import com.aliyun.odps.table.utils.Preconditions;

/**
 * A writer attempt id that is unique to this writer attempt.
 */
public class WriterAttemptId {

    private final int attemptNumber;

    public static WriterAttemptId of(int attemptNumber) {
        return new WriterAttemptId(attemptNumber);
    }

    public WriterAttemptId(int attemptNumber) {
        Preconditions.checkArgument(attemptNumber >= 0, "Attempt number must be a non-negative number.");
        this.attemptNumber = attemptNumber;
    }

    /**
     * How many times this writer has been attempted.
     */
    public int getAttemptNumber() {
        return attemptNumber;
    }
}
