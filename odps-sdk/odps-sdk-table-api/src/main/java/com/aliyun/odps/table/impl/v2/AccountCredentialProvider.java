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

package com.aliyun.odps.table.impl.v2;

import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;
import com.aliyun.odps.account.Account;

/**
 * Adapts a table-api {@link Account} to the credentials API consumed by the Storage API v2
 * SDK. Credentials are re-read from the account per request so STS rotation keeps working.
 */
public class AccountCredentialProvider implements ICredentialsProvider {

    private final Account account;

    public AccountCredentialProvider(Account account) {
        this.account = account;
    }

    @Override
    public ICredentials getCredentials() {
        return account.getCredentials();
    }

    @Override
    public String getProviderName() {
        return "odps-account";
    }

    @Override
    public void close() {
        // The account lifecycle is owned by EnvironmentSettings.
    }
}
