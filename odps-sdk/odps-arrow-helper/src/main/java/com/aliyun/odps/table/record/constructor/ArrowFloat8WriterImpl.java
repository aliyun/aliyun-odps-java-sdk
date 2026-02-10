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
import com.aliyun.odps.table.arrow.constructor.ArrowFloat8Writer;
import org.apache.arrow.vector.Float8Vector;

import java.util.List;

public class ArrowFloat8WriterImpl {

    public static final class RecordFloat8Writer extends ArrowFloat8Writer<ArrayRecord> {

        RecordFloat8Writer(Float8Vector float8Vector) {
            super(float8Vector);
        }

        @Override
        protected boolean isNullAt(ArrayRecord row, int ordinal) {
            return row.isNull(ordinal);
        }

        @Override
        protected double readDouble(ArrayRecord row, int ordinal) {
            return row.getDouble(ordinal);
        }
    }

    public static final class ListFloat8Writer extends ArrowFloat8Writer<List<Object>> {

        ListFloat8Writer(Float8Vector float8Vector) {
            super(float8Vector);
        }

        @Override
        protected boolean isNullAt(List<Object> in, int ordinal) {
            return in.get(ordinal) == null;
        }

        @Override
        protected double readDouble(List<Object> in, int ordinal) {
            return (double) in.get(ordinal);
        }
    }

    public static final class StructFloat8Writer extends ArrowFloat8Writer<Struct> {

        StructFloat8Writer(Float8Vector float8Vector) {
            super(float8Vector);
        }

        @Override
        protected boolean isNullAt(Struct in, int ordinal) {
            return in.getFieldValue(ordinal) == null;
        }

        @Override
        protected double readDouble(Struct in, int ordinal) {
            return (double) in.getFieldValue(ordinal);
        }
    }
}
