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

package com.aliyun.odps.table.read;

import com.aliyun.odps.data.JsonSerializable;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.Session;

public interface TableReadSession extends Session, JsonSerializable {

    /**
     * Returns the actual schema of this table read session
     */
    DataSchema readSchema();

    /**
     * Returns whether the data format is supported by this table read session.
     */
    boolean supportsDataFormat(DataFormat dataFormat);
}
