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

package com.aliyun.odps.tunnel.io;

import java.util.List;
import com.aliyun.odps.data.Record;
@Deprecated
public class ReadPackResult {
    private String currPackId;
    private String nextPackId;
    private long   timeStamp;
    private List<Record> records;
    private byte [] meta;

    public ReadPackResult(String packId, String nextPackId, long timeStamp, List<Record> records) {
      this(packId, nextPackId, timeStamp, records, null);
    }
    
    public ReadPackResult(String packId, String nextPackId, long timeStamp, List<Record> records, byte [] meta) {
        if (packId == null || nextPackId == null) {
            throw new IllegalArgumentException("Invalid pack string.");
        }
        this.currPackId = packId;
        this.nextPackId = nextPackId;
        this.timeStamp = timeStamp;
        this.records = records;
        this.meta = meta;
    }

    public List<Record> getRecords() {
        return this.records;
    }

    public long getTimeStamp() {
        return this.timeStamp;
    }

    public String getPackId() {
        return this.currPackId;
    }

    public String getNextPackId() {
        return this.nextPackId;
    }
    
    public byte [] getMeta() {
        return this.meta;
    }
}
