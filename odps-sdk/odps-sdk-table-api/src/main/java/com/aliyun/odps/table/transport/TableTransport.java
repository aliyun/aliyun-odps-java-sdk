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
import com.aliyun.odps.commons.transport.DefaultTransport;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.transport.Transport;
import com.aliyun.odps.commons.util.IOUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Proxy;

/** Selects the HTTP transport used by every Table API request. */
public final class TableTransport implements Transport {

    private final Transport jdkTransport;
    private final Integer writeTimeoutSeconds;

    public TableTransport(@Nullable Integer writeTimeoutSeconds) {
        this(new DefaultTransport(), writeTimeoutSeconds);
    }

    TableTransport(Transport jdkTransport, @Nullable Integer writeTimeoutSeconds) {
        this.jdkTransport = jdkTransport;
        this.writeTimeoutSeconds = writeTimeoutSeconds;
    }

    @Override
    public Response request(Request request) throws IOException {
        if (writeTimeoutSeconds == null) {
            return jdkTransport.request(request);
        }

        Connection connection = connect(request);
        try {
            if (request.getBody() != null) {
                OutputStream output = connection.getOutputStream();
                IOUtils.copyLarge(request.getBody(), output);
                output.close();
            }

            Response response = connection.getResponse();
            byte[] body = null;
            if (request.getMethod() != Request.Method.HEAD) {
                InputStream input = connection.getInputStream();
                body = IOUtils.readFully(input);
            }
            return new BufferedResponse(response, body);
        } finally {
            connection.disconnect();
        }
    }

    @Override
    public Connection connect(Request request) throws IOException {
        Connection connection = writeTimeoutSeconds == null
                ? new JdkConnection(jdkTransport)
                : new OkHttpConnection(writeTimeoutSeconds);
        connection.connect(request);
        return connection;
    }

    @Override
    public void setProxy(Proxy proxy) {
        jdkTransport.setProxy(proxy);
    }

    private static final class BufferedResponse extends Response {

        private BufferedResponse(Response response, byte[] body) {
            this.status = response.getStatus();
            this.message = response.getMessage();
            this.headers.putAll(response.getHeaders());
            this.body = body;
        }
    }
}
