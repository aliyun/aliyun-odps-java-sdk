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

package com.aliyun.odps.account;

/**
 * This class is for keeping track of the behaviors of a certain application.
 *
 * @author Jon (wangzhong.zw@alibaba-inc.com)
 */
public class AppAccount implements Account {
    private Account appAccount;
    private AppRequestSigner signer;

    public AppAccount(Account account) {
        if (account == null) {
            throw new IllegalArgumentException("Account cannot be null");
        }

        switch (account.getType()) {
            case ALIYUN:
                this.appAccount = account;
                this.signer = new AppRequestSigner((AliyunAccount) account);
                break;
            default:
                throw new IllegalArgumentException("Unsupported account provider for application account.");
        }
    }

    @Override
    public AccountProvider getType() {
        return appAccount.getType();
    }

    @Override
    public AppRequestSigner getRequestSigner() {
        return signer;
    }
}
