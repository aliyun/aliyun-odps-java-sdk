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

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a sort order in a specific column.
 */
public class SortOrder implements Serializable {

    private final String name;
    private final SortDirection direction;
    private final NullOrdering nullOrdering;

    public SortOrder(String name,
                     SortDirection direction,
                     NullOrdering nullOrdering) {
        this.name = name;
        this.direction = direction;
        this.nullOrdering = nullOrdering;
    }

    /**
     * Returns the sort column name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the sort direction.
     */
    public SortDirection direction() {
        return direction;
    }

    /**
     * Returns the null ordering.
     */
    public NullOrdering nullOrdering() {
        return nullOrdering;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SortOrder that = (SortOrder) o;
        return Objects.equals(name, that.name)
                && Objects.equals(direction, that.direction)
                && Objects.equals(nullOrdering, that.nullOrdering);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, direction, nullOrdering);
    }
}
