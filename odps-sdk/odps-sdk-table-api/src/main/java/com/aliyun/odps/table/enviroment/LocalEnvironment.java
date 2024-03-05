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

package com.aliyun.odps.table.enviroment;

import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static com.aliyun.odps.table.utils.ConfigConstants.*;

public class LocalEnvironment extends ExecutionEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(ExecutionEnvironment.class);

    private String localEndpoint = null;

    public LocalEnvironment(EnvironmentSettings settings) {
        super(settings);
    }

    @Override
    protected void initialize() {
        String mode = System.getenv(MAX_STORAGE_MODE);
        if (StringUtils.isNullOrEmpty(mode)) {
            logger.warn("Local env without maxstorage mode");
            return;
        }

        String prefix = System.getenv(MAX_STORAGE_DATA_PROXY_PREFIX);
        String port = System.getenv(MAX_STORAGE_DATA_PROXY_PORT);

        Preconditions.checkString(port, "MAX_STORAGE_DATA_PROXY_PORT");

        String localHostPort;
        if (StringUtils.isNullOrEmpty(prefix)) {
            localHostPort = "http://127.0.0.1:" + port;
        } else {
            localHostPort = "http://127.0.0.1:" + port + "/" + prefix;
        }
        this.localEndpoint = localHostPort;
    }

    @Override
    public String getTunnelEndpoint(String targetProject) {
        ensureInitialized();
        return Optional.ofNullable(localEndpoint).orElseGet(() ->
                settings.getTunnelEndpoint().orElseThrow(() ->
                        new IllegalStateException("Local environment get empty tunnel endpoint!")));
    }
}
