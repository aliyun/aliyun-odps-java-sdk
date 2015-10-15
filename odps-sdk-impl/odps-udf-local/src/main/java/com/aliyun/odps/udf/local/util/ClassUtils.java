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

package com.aliyun.odps.udf.local.util;

import java.lang.reflect.Method;

import com.aliyun.odps.udf.local.LocalRunException;

public class ClassUtils {

  public static Object newInstance(ClassLoader loader, String className) throws LocalRunException {
    try {
      Class<?> tfClz = Class.forName(className, true, loader);
      return tfClz.newInstance();
    } catch (Exception e) {
      throw new LocalRunException(e);
    }
  }

  public static Method findMethod(Class<?> clz, String methodName, Object[] parameters)
      throws LocalRunException {
    Class<?>[] parameterTypes = new Class<?>[parameters.length];
    for (int i = 0; i < parameters.length; ++i) {
      parameterTypes[i] = parameters[i].getClass();
    }
    Method method = null;
    try {
      method = clz.getMethod(methodName, parameterTypes);
    } catch (SecurityException e) {
      throw new LocalRunException(e);
    } catch (NoSuchMethodException e) {
      throw new LocalRunException(e);
    }
    return method;
  }

}
