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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.local.common.security.SecurityClient;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.util.ClassUtils;

public class UDFRunner extends BaseRunner {

  private UDF udf;
  private Method evalMethod;

  public UDFRunner(Odps odps, UDF udf) throws UDFException {
    super(odps);
    if (udf == null) {
      throw new IllegalArgumentException("Missing arguments:udf");
    }
    this.udf = udf;

    try {
      SecurityClient.open();
      udf.setup(context);
    } catch (Exception ex) {
      throw new UDFException(ex);
    } finally {
      SecurityClient.close();
    }
  }

  public UDFRunner(Odps odps, String className) throws LocalRunException {
    super(odps);
    if (StringUtils.isBlank(className)) {
      throw new IllegalArgumentException("Missing arguments:className");
    }
    try {
      udf = (UDF) ClassUtils.newInstance(UDFRunner.class.getClassLoader(), className);

      SecurityClient.open();
      udf.setup(context);

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
    try {
      SecurityClient.open();
      if (evalMethod == null) {
        evalMethod = findMethod(udf.getClass(), input);
      }
      input = ClassUtils.adaptVarargMethodDataIfNecessary(evalMethod, input);
      Object result = evalMethod.invoke(udf, input);
      buffer.add(new Object[] {result});
    } catch (InvocationTargetException e) {
      throw new LocalRunException(e);
    } catch (IllegalArgumentException e) {
      throw new LocalRunException(e);
    } catch (IllegalAccessException e) {
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
      udf.close();
    } catch (UDFException e) {
      throw new LocalRunException(e);
    } finally {
      SecurityClient.close();
    }
    // avoid user to call function again
    udf = null;
    return buffer;
  }

  private Method findMethod(Class<?> clz, Object[] args) throws LocalRunException {
    if (args == null || args.length == 0) {
      throw new LocalRunException("Input data can't be null");
    }
    Method method = ClassUtils.findMethod(clz, "evaluate", args);
    // method "evaluate" can't be static
    if (Modifier.toString(method.getModifiers()).contains("static")) {
      throw new LocalRunException("'evaluate' method can't be static");
    }

    return method;
  }

}
