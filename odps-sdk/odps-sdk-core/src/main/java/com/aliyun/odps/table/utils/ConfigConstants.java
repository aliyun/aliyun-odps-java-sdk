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

package com.aliyun.odps.table.utils;

public class ConfigConstants {

    public static final String DEFAULT_CHARSET = "utf-8";
    public static final String VERSION_1 = "v1";

    public static final String MAX_STORAGE_DATA_PROXY_PREFIX = "MAX_STORAGE_DATA_PROXY_PREFIX";
    public static final String MAX_STORAGE_DATA_PROXY_PORT = "MAX_STORAGE_DATA_PROXY_PORT";
    public static final String MAX_STORAGE_MODE = "MAX_STORAGE_MODE";
    public static final String MAX_STORAGE_VSHM_MODE = "MAX_STORAGE_VSHM_MODE";
    public static final String MAX_STORAGE_NET_MODE = "MAX_STORAGE_NET_MODE";
    public static final String MAX_STORAGE_DATA_PROXY_CONF_PATH = "MAX_STORAGE_DATA_PROXY_CONF_PATH";

    public static final String SESSION_ID = "session_id";
    public static final String SESSION_TYPE = "session_type";
    public static final String SPLIT_INDEX = "split_index";
    public static final String ROW_INDEX = "row_index";
    public static final String ROW_COUNT = "row_count";
    public static final String MAX_BATCH_ROWS = "max_batch_rows";
    public static final String MAX_BATCH_RAW_SIZE = "max_batch_raw_size";

    public static final String BLOCK_NUMBER = "block_number";
    public static final String ATTEMPT_NUMBER = "attempt_number";

    public static final String DATA_FORMAT_TYPE = "data_format_type";
    public static final String DATA_FORMAT_VERSION = "data_format_version";

    public static final long DEFAULT_ASYNC_INTERVAL_IN_MILLS = 3000;
    public static final int DEFAULT_ASYNC_TIMEOUT_IN_SECONDS = 3600;

    public static final int DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024;
    public static final int DEFAULT_BUFFERED_ROW_COUNT = 4096;

}
