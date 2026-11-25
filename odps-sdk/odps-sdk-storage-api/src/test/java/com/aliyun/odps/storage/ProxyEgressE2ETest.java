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

package com.aliyun.odps.storage;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.aliyun.odps.ProxyConfig;
import com.aliyun.odps.credentials.StaticCredentialProvider;
import com.aliyun.odps.storage.settings.HttpSettings;
import com.aliyun.odps.table.TableIdentifier;

/**
 * The storage-api (OkHttp) egress against a real MaxCompute service, with and without an
 * authenticating proxy. Unlike {@code HttpClientProxyAuthTest} (local origin, always green), this
 * class talks to the service the customer talks to, so it is opt-in:
 *
 * <pre>
 *   mvn -pl odps-sdk/odps-sdk-storage-api -am test -Dtest=ProxyEgressE2ETest \
 *       -Dodps.e2e=true \
 *       -Dodps.e2e.proxy.address=http://127.0.0.1:18911        # omit for the direct run
 * </pre>
 *
 * Required environment (injected by the workspace credential wrapper, never stored in a file):
 * {@code ODPS_ACCESS_ID}, {@code ODPS_ACCESS_KEY}, {@code ODPS_ENDPOINT}, {@code ODPS_PROJECT},
 * and for the proxy run {@code ODPS_E2E_PROXY_USER} / {@code ODPS_E2E_PROXY_PASS}.
 *
 * <p>No data is written: the session is created for a table name that does not exist, so the
 * service answers with its own error. A server answer (request id or {@code OdpsError} body) is
 * the proof that the request left the machine through the proxy; a local transport failure is not.
 */
class ProxyEgressE2ETest {

  private static final String E2E_ENABLED = "odps.e2e";
  private static final String PROXY_ADDRESS = "odps.e2e.proxy.address";
  private static final String ABSENT_TABLE = "proxy_e2e_absent_table_do_not_create";

  private static boolean enabled() {
    return Boolean.getBoolean(E2E_ENABLED);
  }

  private static String proxyAddress() {
    return System.getProperty(PROXY_ADDRESS);
  }

  @Test
  void readSessionRequestReachesTheRealService() {
    Assumptions.assumeTrue(enabled(),
        "opt in with -D" + E2E_ENABLED + "=true and a real identity in the environment");
    requireIdentity();

    ProxyConfig.Builder proxy = ProxyConfig.builder();
    String address = proxyAddress();
    boolean proxyMode = address != null && !address.isEmpty();
    String user = System.getenv("ODPS_E2E_PROXY_USER");
    String password = System.getenv("ODPS_E2E_PROXY_PASS");
    if (proxyMode) {
      if (user != null && password != null) {
        proxy.withHttpProxy(address, user, password)
            .withHttpsProxy(address, user, password);
      } else {
        proxy.withHttpProxy(address).withHttpsProxy(address);
      }
    }

    HttpSettings httpSettings = HttpSettings.newBuilder()
        .withConnectTimeout(Duration.ofSeconds(20))
        .withReadTimeout(Duration.ofSeconds(60))
        .withProxyConfig(proxy.build())
        .build();
    assertNotNull(httpSettings.getProxyConfig(), "the proxy config must reach HttpSettings");

    Throwable thrown = attemptReadSession(httpSettings);
    String outcome;
    if (thrown == null) {
      outcome = "session-created";
    } else {
      // Proof that the request left the machine: the service minted its own request id and answered
      // with its own status/error code. A transport failure carries neither.
      ServiceException se = findServiceException(thrown);
      assertNotNull(se, "the MaxCompute service itself must have answered, got instead: "
          + trim(describe(thrown)));
      assertNotNull(se.getRequestId(), "a server-minted request id is expected");
      assertTrue(se.getHttpStatus() >= 400, "the absent table must be refused by the service, "
          + "http status was " + se.getHttpStatus());
      outcome = "answered status=" + se.getHttpStatus() + " code=" + se.getErrorCode()
          + " requestId=" + se.getRequestId();
    }
    System.out.println("E2E-CELL cell=storageapi-read-session mode="
        + (proxyMode ? "proxy" : "direct") + " outcome=" + outcome);
  }

  /**
   * Counter-case for the same real path: an address-only {@link ProxyConfig} must not be able to
   * pass an authenticating proxy. Direct runs skip this cell (there is no proxy to refuse).
   */
  @Test
  void credentiallessProxyConfigIsRefusedByTheRealProxy() {
    Assumptions.assumeTrue(enabled(), "opt in with -D" + E2E_ENABLED + "=true");
    String address = proxyAddress();
    Assumptions.assumeTrue(address != null && !address.isEmpty(),
        "only meaningful with a proxy: pass -D" + PROXY_ADDRESS + "=...");
    requireIdentity();

    // A dedicated address (see the driver): a refusal cell must not be able to inherit credentials
    // that an earlier cell left in a per-address cache.
    String refusalAddress = System.getProperty("odps.e2e.proxy.refusal.address", address);
    HttpSettings httpSettings = HttpSettings.newBuilder()
        .withConnectTimeout(Duration.ofSeconds(20))
        .withReadTimeout(Duration.ofSeconds(60))
        .withProxyConfig(ProxyConfig.builder()
            .withHttpProxy(refusalAddress).withHttpsProxy(refusalAddress)
            .build())
        .build();

    Throwable thrown = attemptReadSession(httpSettings);
    assertNotNull(thrown, "an authenticating proxy must refuse a credentialless client");
    String text = describe(thrown);
    assertTrue(text.contains("407") || text.toLowerCase().contains("proxy authentication"),
        "the failure must be the proxy refusing, not something else: " + trim(text));
    System.out.println("E2E-CELL cell=storageapi-no-credential mode=proxy outcome=refused "
        + trim(text));
  }

  /**
   * Runs the whole client lifecycle (endpoint discovery included: it is itself a request that must
   * clear the proxy) and reports whatever it threw, so that a refusal at any stage is judged the
   * same way instead of escaping the assertion block.
   */
  private static Throwable attemptReadSession(HttpSettings httpSettings) {
    String project = System.getenv("ODPS_PROJECT");
    try (MaxStorageClient client = MaxStorageClient.builder()
        .endpoint(System.getenv("ODPS_ENDPOINT"))
        .project(project)
        .credentialsProvider(new StaticCredentialProvider(
            System.getenv("ODPS_ACCESS_ID"), System.getenv("ODPS_ACCESS_KEY")))
        .httpSettings(httpSettings)
        .build()) {
      client.createTableReadSessionBuilder(TableIdentifier.of(project, "default", ABSENT_TABLE))
          .build();
      return null;
    } catch (Throwable t) {
      return t;
    }
  }

  private static ServiceException findServiceException(Throwable e) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof ServiceException) {
        return (ServiceException) t;
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return null;
  }

  private static void requireIdentity() {
    String ak = System.getenv("ODPS_ACCESS_ID");
    String sk = System.getenv("ODPS_ACCESS_KEY");
    String endpoint = System.getenv("ODPS_ENDPOINT");
    String project = System.getenv("ODPS_PROJECT");
    Assumptions.assumeTrue(ak != null && sk != null && endpoint != null && project != null,
        "ODPS_ACCESS_ID/ODPS_ACCESS_KEY/ODPS_ENDPOINT/ODPS_PROJECT must be injected");
  }

  private static String describe(Throwable e) {
    StringBuilder sb = new StringBuilder();
    for (Throwable t = e; t != null && sb.length() < 2000; t = t.getCause()) {
      sb.append(t.getClass().getName()).append(": ").append(t.getMessage()).append(' ');
    }
    return sb.toString();
  }

  private static String trim(String s) {
    String one = s.replace('\n', ' ').replace('\r', ' ');
    return one.length() > 180 ? one.substring(0, 180) : one;
  }
}
