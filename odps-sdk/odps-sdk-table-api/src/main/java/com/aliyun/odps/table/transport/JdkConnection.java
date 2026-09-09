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

package com.aliyun.odps.table.transport;

import com.aliyun.odps.commons.transport.Connection;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.transport.Transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** JDK HTTP connection used by Table API when no write timeout is configured. */
final class JdkConnection implements Connection {

    private final Transport transport;
    private Connection delegate;

    JdkConnection(Transport transport) {
        this.transport = transport;
    }

    @Override
    public void connect(Request request) throws IOException {
        delegate = transport.connect(request);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return connection().getOutputStream();
    }

    @Override
    public Response getResponse() throws IOException {
        return connection().getResponse();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return connection().getInputStream();
    }

    @Override
    public void disconnect() throws IOException {
        if (delegate != null) {
            delegate.disconnect();
        }
    }

    private Connection connection() throws IOException {
        if (delegate == null) {
            throw new IOException("JDK connection has not been connected");
        }
        return delegate;
    }
}
