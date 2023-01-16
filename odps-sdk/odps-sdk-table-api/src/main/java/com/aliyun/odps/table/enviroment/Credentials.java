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

import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AppAccount;
import com.aliyun.odps.account.AppStsAccount;
import com.aliyun.odps.table.utils.Preconditions;

import java.io.Serializable;
import java.util.Optional;

/**
 * ODPS credentials for table read/write
 */
public class Credentials {

    private Account account;
    private AppAccount appAccount;
    private AppStsAccount appStsAccount;

    public Account getAccount() {
        return account;
    }

    public Optional<AppAccount> getAppAccount() {
        return Optional.ofNullable(appAccount);
    }

    public Optional<AppStsAccount> getAppStsAccount() {
        return Optional.ofNullable(appStsAccount);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private final Credentials credentials;

        public Builder() {
            this.credentials = new Credentials();
        }

        public Builder withAccount(Account account) {
            this.credentials.account = account;
            return this;
        }

        public Builder withAppAccount(AppAccount appAccount) {
            this.credentials.appAccount = appAccount;
            return this;
        }

        public Builder withAppStsAccount(AppStsAccount appStsAccount) {
            this.credentials.appStsAccount = appStsAccount;
            return this;
        }

        public Credentials build() {
            Preconditions.checkNotNull(this.credentials.getAccount(), "Odps account cannot be null");
            return this.credentials;
        }
    }

}
