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

package com.aliyun.odps.table.order;

/**
 * A null order used in a specific column.
 */
public enum NullOrdering {

    NULLS_FIRST,

    NULLS_LAST,

    ANY;

    @Override
    public String toString() {
        switch (this) {
            case NULLS_FIRST:
                return "NULLS FIRST";
            case NULLS_LAST:
                return "NULLS LAST";
            case ANY:
                return "*";
            default:
                throw new IllegalArgumentException("Unexpected null order");
        }
    }

}
