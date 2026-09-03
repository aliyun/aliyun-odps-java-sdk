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


package com.aliyun.odps.table.metrics;

public class MetricNames {

    private MetricNames() {}

    public static final String RECORD_COUNT = "recordCount";
    public static final String BYTES_COUNT = "bytesCount";

    public static final String SERVER_PROCESS_COST = "serverProcessCost";
    public static final String RATE_LIMIT_COST = "rateLimitCost";

    /** Read-only Counter and Gauge: cumulative Arrow record batches spilled by one reader. */
    public static final String DISK_SPILL_BATCH_COUNT = "diskSpillBatchCount";
    /** Read-only Counter and Gauge: cumulative physical Arrow IPC bytes written by one reader. */
    public static final String DISK_SPILL_BYTES_WRITTEN = "diskSpillBytesWritten";
    /** Read-only Gauge: current physical spill-file bytes held by one reader. */
    public static final String DISK_SPILL_BYTES_IN_USE = "diskSpillBytesInUse";
    /** Read-only Gauge: peak physical spill-file bytes held by one reader. */
    public static final String DISK_SPILL_PEAK_BYTES_IN_USE = "diskSpillPeakBytesInUse";
    /** Read-only Gauge: current spill-file count held by one reader. */
    public static final String DISK_SPILL_FILE_COUNT = "diskSpillFileCount";
    /** Read-only Gauge: 1 after the producer no longer reads from the delegate; otherwise 0. */
    public static final String DISK_SPILL_PRODUCER_FINISHED = "diskSpillProducerFinished";

}
