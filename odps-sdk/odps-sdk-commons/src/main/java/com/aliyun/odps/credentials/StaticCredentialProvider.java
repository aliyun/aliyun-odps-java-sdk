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

package com.aliyun.odps.credentials;


import com.aliyun.credentials.api.ICredentials;
import com.aliyun.credentials.api.ICredentialsProvider;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class StaticCredentialProvider implements ICredentialsProvider {

  private final ICredentials credentials;

  public StaticCredentialProvider(String accessKeyId, String accessKeySecret) {
    this.credentials = new Credentials(accessKeyId, accessKeySecret, null);
  }

  public StaticCredentialProvider(ICredentials credentials) {
    this.credentials = credentials;
  }

  public static StaticCredentialProvider of(String accessKeyId, String accessKeySecret) {
    return new StaticCredentialProvider(accessKeyId, accessKeySecret);
  }

  public static StaticCredentialProvider of(ICredentials credentials) {
    return new StaticCredentialProvider(credentials);
  }


  @Override
  public ICredentials getCredentials() {
    return credentials;
  }

  @Override
  public String getProviderName() {
    return this.credentials != null ? this.credentials.getProviderName() : null;
  }

  @Override
  public void close() {
  }
}
