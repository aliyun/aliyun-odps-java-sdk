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

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.local.common.WareHouse;

import java.io.IOException;
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
  private TableInfo tableInfo;
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
    this(odps, className, null);
  }

  public UDFRunner(Odps odps, String className, TableInfo tableInfo) throws LocalRunException {
    super(odps);
    if (StringUtils.isBlank(className)) {
      throw new IllegalArgumentException("Missing arguments:className");
    }
    this.tableInfo = tableInfo;
    try {
      udf = (UDF) ClassUtils.newInstance(UDFRunner.class.getClassLoader(), className);
      SecurityClient.open();
      udf.setup(context);
    } catch (LocalRunException e) {
      throw e;
    } catch (UDFException e) {
      throw new LocalRunException(e);
    } catch (IOException e) {
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
    } catch (IOException e) {
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
    Class[] parameterTypes;
    if (tableInfo != null) { //use table meta as UDF parameter types
      parameterTypes = WareHouse.getInstance().getColumnTypes(tableInfo.getProjectName(),
        tableInfo.getTableName(), tableInfo.getCols());
    } else { //use data types as UDF parameter types
      parameterTypes = new Class<?>[args.length];
      for (int i = 0; i < args.length; ++i) {
        parameterTypes[i] = args[i].getClass();
      }
    }
    Method method = ClassUtils.findMethod(clz, "evaluate", parameterTypes);
    // method "evaluate" can't be static
    if (Modifier.toString(method.getModifiers()).contains("static")) {
      throw new LocalRunException("'evaluate' method can't be static");
    }

    return method;
  }

}
