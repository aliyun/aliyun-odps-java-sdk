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

package com.aliyun.odps;

import static org.junit.Assert.assertEquals;

import com.aliyun.odps.commons.transport.Headers;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class UserAgentSuffixTest {
  @Test
  public void suffixIsAppendedAsSuppliedToBothExistingHeaders() {
    Odps odps = RequestMetadataTest.client();
    String base = RequestMetadataTest.headers(odps).get(Headers.USER_AGENT);
    String suffix = " agentic/2 (source=Custom; future=value) "
        + String.join("", Collections.nCopies(2048, "x")) + " ";
    odps.options().setUserAgentSuffix(suffix);

    Map<String, String> headers = RequestMetadataTest.headers(odps);
    assertEquals(base + " " + suffix, headers.get(Headers.USER_AGENT));
    assertEquals(base + " " + suffix, headers.get(Headers.ODPS_USER_AGENT));
  }

  @Test
  public void whitespaceIsPreservedAndEmptyStringClears() {
    Odps odps = RequestMetadataTest.client();
    String base = RequestMetadataTest.headers(odps).get(Headers.USER_AGENT);
    odps.options().setUserAgentSuffix("   ");
    assertEquals(base + "    ", RequestMetadataTest.headers(odps).get(Headers.USER_AGENT));

    odps.options().setUserAgentSuffix("");
    assertEquals(base, RequestMetadataTest.headers(odps).get(Headers.USER_AGENT));
  }
}
