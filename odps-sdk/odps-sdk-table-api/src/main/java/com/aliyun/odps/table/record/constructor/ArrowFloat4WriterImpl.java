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

package com.aliyun.odps.table.record.constructor;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.arrow.constructor.ArrowFloat4Writer;
import org.apache.arrow.vector.Float4Vector;

import java.util.List;

public class ArrowFloat4WriterImpl {

    public static final class RecordFloat4Writer extends ArrowFloat4Writer<ArrayRecord> {

        RecordFloat4Writer(Float4Vector float4Vector) {
            super(float4Vector);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }

        @Override
        protected float readFloat(ArrayRecord row, int ordinal) {
            return row.getFloat(ordinal);
        }
    }

    public static final class ListFloat4Writer extends ArrowFloat4Writer<List<Object>> {

        ListFloat4Writer(Float4Vector float4Vector) {
            super(float4Vector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected float readFloat(List<Object> in, int ordinal) {
            return (float) in.get(ordinal);
        }
    }

    public static final class StructFloat4Writer extends ArrowFloat4Writer<Struct> {

        StructFloat4Writer(Float4Vector float4Vector) {
            super(float4Vector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected float readFloat(Struct in, int ordinal) {
            return (float) in.getFieldValue(ordinal);
        }
    }
}
