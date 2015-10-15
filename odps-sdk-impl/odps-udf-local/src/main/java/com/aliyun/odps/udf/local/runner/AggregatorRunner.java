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

package com.aliyun.odps.udf.local.runner;

import java.util.List;

import com.aliyun.odps.Odps;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.local.common.security.SecurityClient;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.util.ClassUtils;
import com.aliyun.odps.udf.local.util.LocalWritableUtils;

public class AggregatorRunner extends BaseRunner {

  private Aggregator aggregator;
  private Writable userDefineBuffer;

  public AggregatorRunner(Odps odps, Aggregator aggregator) throws UDFException {
    super(odps);
    this.aggregator = aggregator;
    try {
      SecurityClient.open();
      aggregator.setup(context);
      userDefineBuffer = aggregator.newBuffer();
    } catch (Exception ex) {
      throw new UDFException(ex);
    } finally {
      SecurityClient.close();
    }
  }

  public AggregatorRunner(Odps odps, String className) throws LocalRunException {
    super(odps);
    try {
      SecurityClient.open();
      aggregator =
          (Aggregator) ClassUtils.newInstance(AggregatorRunner.class.getClassLoader(), className);
      aggregator.setup(context);
      userDefineBuffer = aggregator.newBuffer();
    } catch (LocalRunException e) {
      throw e;
    } catch (UDFException e) {
      throw new LocalRunException(e);
    } finally {
      SecurityClient.close();
    }

  }

  @Override
  public BaseRunner internalFeed(Object[] input) throws LocalRunException {
    Writable[] inputWritables = LocalWritableUtils.convert(input);
    try {
      SecurityClient.open();
      Writable partialBuffer = aggregator.newBuffer();
      aggregator.iterate(partialBuffer, inputWritables);
      aggregator.merge(userDefineBuffer, partialBuffer);
    } catch (UDFException e) {
      throw new LocalRunException(e);
    } finally {
      SecurityClient.close();
    }
    return this;
  }

  @Override
  public List<Object[]> internalYield() throws LocalRunException {
    try {
      SecurityClient.open();
      Writable result = aggregator.terminate(userDefineBuffer);
      buffer.add(new Object[] {LocalWritableUtils.convert(result)});
      aggregator.close();
    } catch (UDFException e) {
      throw new LocalRunException(e);
    } finally {
      SecurityClient.close();
    }
    // avoid user to call function again
    aggregator = null;
    return buffer;
  }

}
