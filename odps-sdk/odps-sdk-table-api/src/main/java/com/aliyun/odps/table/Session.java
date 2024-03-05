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

public interface Session extends Serializable {

    /**
     * Returns the id for table session.
     */
    String getId();

    /**
     * Returns the table identifier for table session.
     */
    TableIdentifier getTableIdentifier();

    /**
     * Returns the type for table session.
     */
    SessionType getType();

    /**
     * Returns the status for table session.
     */
    SessionStatus getStatus();

    /**
     * A description string of this session
     */
    default String description() {
        return this.getClass().toString();
    }

    /**
     * Base interface for all kind of session providers.
     * A session provider is uniquely identified by {@link Class} and {@link #identifier()}.
     */
    interface Provider {
        /**
         * A description string of this session provider,
         */
        String identifier();
    }
}
