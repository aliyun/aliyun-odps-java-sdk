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

package com.aliyun.odps.table.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.Closeable;
import java.io.IOException;

/**
 * An arrow batch reader that reads into VectorSchemaRoot directly.
 */
public interface ArrowReader extends Closeable {

    /**
     * Return the current arrow batch. This method should return same value until `nextBatch` is called.
     */
    VectorSchemaRoot getCurrentValue();

    /**
     * Proceed to next arrow batch, returns false if there is no more batches.
     *
     * @throws IOException if failure happens during network IO.
     */
    boolean nextBatch() throws IOException;

    /**
     * Return the current read bytes.
     */
    long bytesRead();
}
