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

package com.aliyun.odps.mapred.bridge;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Resources;

public class MetaExplorerImplTest {

  @Test
  public void testAddTempResourceWithRetry() throws OdpsException {
    Resources resHandler = mock(Resources.class);
    Odps odps = mock(Odps.class);
    when(odps.resources()).thenReturn(resHandler);
    Mockito.doThrow(new OdpsException("Here is the timeout u want.")).doNothing().when(resHandler)
        .create(any(FileResource.class), any(InputStream.class));

    MetaExplorer me = new MetaExplorerImpl(odps);

    String name = me.addTempResourceWithRetry(new ByteArrayInputStream(new byte[]{}), "foo",
                                              Resource.Type.FILE);
    Assert.assertEquals("foo_1", name);
  }
}
