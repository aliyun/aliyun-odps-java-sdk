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

import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.Session;
import com.aliyun.odps.table.distribution.Distribution;
import com.aliyun.odps.table.distribution.UnspecifiedDistribution;
import com.aliyun.odps.table.order.SortOrder;

import java.util.Optional;

/**
 * A logical representation of a maxcompute table sink.
 */
public interface TableWriteSession extends Session {

    /**
     * Returns the schema for table write session.
     */
    DataSchema requiredSchema();

    /**
     * Returns whether the data format is supported by this table write session.
     */
    boolean supportsDataFormat(DataFormat dataFormat);

    /**
     * Returns the max parallelism required by this table write session.
     *
     * @return empty if the table sink does not provide a max parallelism
     */
    default Optional<Long> maxBlockNumber() {
        return Optional.empty();
    }

    /**
     * Returns the distribution required by this table write session.
     *
     * @return {@link UnspecifiedDistribution} if the table sink does not provide a distribution
     */
    default Distribution requiredDistribution() {
        return new UnspecifiedDistribution();
    }

    /**
     * Returns the ordering required by this table write session.
     *
     * @return empty sort order if the table sink does not provide a sort order
     */
    default SortOrder[] requiredOrdering() {
        return new SortOrder[]{};
    }
}
