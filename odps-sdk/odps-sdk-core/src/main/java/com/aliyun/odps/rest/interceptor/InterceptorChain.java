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

package com.aliyun.odps.rest.interceptor;


import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.commons.transport.Response;

import java.util.ArrayList;
import java.util.List;

public class InterceptorChain implements AutoCloseable {
    private final List<RequestInterceptor> requestInterceptors = new ArrayList<>();
    private final List<ResponseInterceptor> responseInterceptors = new ArrayList<>();

    private InterceptorChain() {
    }

    public static InterceptorChain create() {
        return new InterceptorChain();
    }

    public void addRequestInterceptor(RequestInterceptor interceptor) {
        this.requestInterceptors.add(interceptor);
    }

    public void addResponseInterceptor(ResponseInterceptor interceptor) {
        this.responseInterceptors.add(interceptor);
    }

    @Override
    public void close() {
        requestInterceptors.clear();
        responseInterceptors.clear();
    }

    public void modifyRequest(InterceptorContext context) {
        for (RequestInterceptor interceptor : requestInterceptors) {
            Request interceptorResult = interceptor.modifyRequest(context);
            context.setRequest(interceptorResult);
        }
    }

    public void modifyResponse(InterceptorContext context) {
        for (ResponseInterceptor interceptor : responseInterceptors) {
            Response interceptorResult = interceptor.modifyResponse(context);
            context.setResponse(interceptorResult);
        }
    }
}
