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

import com.aliyun.odps.local.common.Pair;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.udf.local.util.ArgumentConverterUtils.AnyConverter;
import com.aliyun.odps.udf.local.util.ResolveUtils;
import java.io.IOException;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.local.common.security.SecurityClient;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.datasource.UDTFStdoutCollector;
import com.aliyun.odps.udf.local.util.ArgumentConverterUtils;
import com.aliyun.odps.udf.local.util.ArgumentConverterUtils.ArgumentConverter;
import com.aliyun.odps.udf.local.util.ClassUtils;

public class UDTFRunner extends BaseRunner {

  private UDTF tf;
  private ArgumentConverter[] converters;
  private boolean hasAnyArgument;

  public UDTFRunner(Odps odps, UDTF udtf) throws LocalRunException, UDFException {
    super(odps);
    this.tf = udtf;
    tf.setCollector(new UDTFStdoutCollector(buffer));
    checkArguments(tf);

    try {
      SecurityClient.open();
      tf.setup(context);
    } catch (Exception ex) {
      throw new UDFException(ex);
    } finally {
      SecurityClient.close();
    }
  }

  public UDTFRunner(Odps odps, String className) throws LocalRunException, UDFException {
    super(odps);
    if (StringUtils.isBlank(className)) {
      throw new IllegalArgumentException("Missing arguments:className");
    }
    tf = (UDTF) ClassUtils.newInstance(UDTFRunner.class.getClassLoader(), className);
    tf.setCollector(new UDTFStdoutCollector(buffer));
    checkArguments(tf);
    try {
      SecurityClient.open();
      tf.setup(context);
    } catch (Exception ex) {
      throw new UDFException(ex);
    } finally {
      SecurityClient.close();
    }
  }

  private void checkArguments(UDTF tf) {
    Resolve r = tf.getClass().getAnnotation(Resolve.class);
    Pair<List<TypeInfo>, List<TypeInfo>> inputOutputTypes = ResolveUtils.parseResolve(r);
    List<TypeInfo> inputTypes = inputOutputTypes.getFirst();
    converters = new ArgumentConverter[inputTypes.size()];
    for (int i = 0; i < inputTypes.size(); i++) {
      String sigType = ArgumentConverterUtils.getSigType(inputTypes.get(i));
      converters[i] = ArgumentConverterUtils.validSigType.get(sigType);
      if (converters[i] instanceof AnyConverter) {
        hasAnyArgument = true;
      }
    }
  }

  @Override
  public BaseRunner internalFeed(Object[] input) throws LocalRunException {
    if (hasAnyArgument ? input.length < converters.length : input.length != converters.length) {
      throw new LocalRunException("Input column count expected:" + converters.length
          + ", while is:" + input.length);
    }

    for (int i = 0; i < converters.length; i++) {
      converters[i].check(input[i]);
    }
    try {
      SecurityClient.open();
      tf.process(input);
    } catch (UDFException e) {
      throw new LocalRunException(e);
    } catch (IOException e) {
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
      tf.close();
    } catch (UDFException e) {
      throw new LocalRunException(e);
    } finally {
      SecurityClient.close();
    }
    // avoid user to call function again
    tf = null;
    return buffer;
  }

}
