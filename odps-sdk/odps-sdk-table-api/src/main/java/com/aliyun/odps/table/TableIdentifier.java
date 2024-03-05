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

import com.aliyun.odps.table.utils.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/**
 * Identifier of table.
 */
public class TableIdentifier implements Serializable {

    private final String project;
    private final String schema;
    private final String table;

    public static TableIdentifier of(String project, String table) {
        return new TableIdentifier(project, table);
    }

    public static TableIdentifier of(String project, String schema, String table) {
        return new TableIdentifier(project, schema, table);
    }

    public TableIdentifier(String project, String table) {
        this(project, "default", table);
    }

    public TableIdentifier(String project, String schema, String table) {
        Preconditions.checkString(project, "Identifier project cannot be null");
        Preconditions.checkString(table, "Identifier table cannot be null");
        this.project = project;
        this.schema = schema;
        this.table = table;
    }

    public String getProject() {
        return project;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    @Override
    public String toString() {
        return String.join(".", project, schema, table);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TableIdentifier that = (TableIdentifier) o;
        return project.equals(that.project)
                && schema.equals(that.schema)
                && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project, schema, table);
    }
}
