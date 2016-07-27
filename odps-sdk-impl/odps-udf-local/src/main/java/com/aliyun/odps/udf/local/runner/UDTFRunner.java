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

import java.io.IOException;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.local.common.security.SecurityClient;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.udf.local.InvalidFunctionException;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.datasource.UDTFStdoutCollector;
import com.aliyun.odps.udf.local.util.ArgumentConverterUtils;
import com.aliyun.odps.udf.local.util.ArgumentConverterUtils.ArgumentConverter;
import com.aliyun.odps.udf.local.util.ClassUtils;

public class UDTFRunner extends BaseRunner {

  private UDTF tf;
  private ArgumentConverter[] converters;

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

  private void checkArguments(UDTF tf) throws LocalRunException {
    Resolve r = tf.getClass().getAnnotation(Resolve.class);
    if (r == null) {
      throw new LocalRunException("You must specify @Resolve annotation.");
    }
    String info = r.value()[0];
    String[] outs = parseResolutionInfo(info);
    String[] inputs = StringUtils.splitPreserveAllTokens(outs[0], ',');
    converters = new ArgumentConverter[inputs.length];
    for (int i = 0; i < inputs.length; i++) {
      converters[i] = ArgumentConverterUtils.validSigType.get(inputs[i]);
    }
  }

  private String[] parseResolutionInfo(String info) throws InvalidFunctionException {
    String errMsg = "@Resolve({\"" + info + "\"}) ";
    if (info.isEmpty()) {
      throw new InvalidFunctionException(errMsg + "must not be empty string");
    }
    int pos = info.indexOf("->");
    String args = "";
    if (pos > 0) {
      args = info.substring(0, pos);
    } else if (pos < 0) {
      throw new InvalidFunctionException(errMsg);
    }
    String rtypes;
    int tPos = info.indexOf("->", pos + 2);
    if (tPos >= 0) {
      throw new InvalidFunctionException(errMsg + "contains not exactly one '->'");
    }
    rtypes = info.substring(pos + 2, info.length());
    if (!validTypeInfo(args)) {
      throw new InvalidFunctionException(errMsg + "annotates wrong arguments '" + args + "'");
    }
    if (rtypes.isEmpty()) {
      throw new InvalidFunctionException(errMsg + "annotates no output types '" + args + "'");
    } else if (!validTypeInfo(rtypes)) {
      throw new InvalidFunctionException(errMsg + "annotates wrong output types '" + rtypes + "'");
    }
    return new String[] {args, rtypes};
  }

  public static boolean validTypeInfo(String sig) {
    if (sig.isEmpty()) {
      return true;
    }
    String[] sigArray = StringUtils.splitPreserveAllTokens(sig, ',');
    for (String type : sigArray) {
      if (!ArgumentConverterUtils.validSigType.containsKey(type)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public BaseRunner internalFeed(Object[] input) throws LocalRunException {
    if (input.length != converters.length) {
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
