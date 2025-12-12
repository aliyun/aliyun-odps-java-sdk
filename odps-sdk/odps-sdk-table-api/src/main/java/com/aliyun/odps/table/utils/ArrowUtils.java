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

import com.aliyun.odps.table.DataFormat;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.util.TransferPair;

public final class ArrowUtils {

    private static RootAllocator rootAllocator;

    public static synchronized RootAllocator getDefaultRootAllocator() {
        if (rootAllocator == null) {
            rootAllocator = new RootAllocator(Long.MAX_VALUE);
        }
        return rootAllocator;
    }

    public static DataFormat getDefaultDataFormat() {
        return new DataFormat(DataFormat.Type.ARROW, DataFormat.Version.V5);
    }

    /**
     * Returns a new allocator limited to maxAllocation bytes
     */
    public static RootAllocator newRootAllocator(long maxAllocation) {
        return new RootAllocator(
                RootAllocator.configBuilder()
                        .maxAllocation(maxAllocation)
                        .build());
    }

    /**
     * Decodes a dictionary encoded array using the provided dictionary.
     *
     * @param indices dictionary encoded values, must be int type
     * @param dictionary dictionary used to decode the values
     * @return vector with values restored from dictionary
     */
    public static ValueVector decode(ValueVector indices, Dictionary dictionary) {
        int count = indices.getValueCount();
        ValueVector dictionaryVector = dictionary.getVector();
        int dictionaryCount = dictionaryVector.getValueCount();
        // copy the dictionary values into the decoded vector
        TransferPair transfer = dictionaryVector.getTransferPair(indices.getName(), indices.getAllocator());
        transfer.getTo().allocateNewSafe();
        try {
            BaseIntVector baseIntVector = (BaseIntVector) indices;
            retrieveIndexVector(baseIntVector, transfer, dictionaryCount, 0, count);
            ValueVector decoded = transfer.getTo();
            decoded.setValueCount(count);
            return decoded;
        } catch (Exception e) {
            AutoCloseables.close(e, transfer.getTo());
            throw e;
        }
    }

    /**
     * Retrieve values to target vector from index vector.
     *
     * @param indices the index vector
     * @param transfer the {@link TransferPair} to copy dictionary data into target vector.
     * @param dictionaryCount the value count of dictionary vector.
     * @param start the start index
     * @param end the end index
     */
    public static void retrieveIndexVector(
            BaseIntVector indices, TransferPair transfer, int dictionaryCount, int start, int end) {
        for (int i = start; i < end; i++) {
            if (!indices.isNull(i)) {
                int indexAsInt = (int) indices.getValueAsLong(i);
                if (indexAsInt >= dictionaryCount) {
                                    throw new IllegalArgumentException(
                                            "Provided dictionary does not contain value for index " + indexAsInt);
                                }
                transfer.copyValueSafe(indexAsInt, i);
            }
        }
    }

    public static boolean hasDictionaryEncoding(FieldVector fieldVector) {
        return fieldVector.getField().getDictionary() != null ||
                fieldVector.getChildrenFromFields().stream()
                        .anyMatch(ArrowUtils::hasDictionaryEncoding);
    }
}
