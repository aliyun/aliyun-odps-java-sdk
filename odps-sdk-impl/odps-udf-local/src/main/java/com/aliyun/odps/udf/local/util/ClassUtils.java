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

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

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

  public static Method findMethod(Class<?> clz, String methodName, Class[] parameterTypes)
      throws LocalRunException {
    Method method;
    try {
      method = clz.getMethod(methodName, parameterTypes);
    } catch (SecurityException e) {
      throw new LocalRunException(e);
    } catch (NoSuchMethodException e) {
      // try to find variable arguments method
      method = findVarargMethod(clz, methodName, parameterTypes);
      if (method == null) {
        throw new LocalRunException(e);
      }

    }
    return method;
  }

  public static Method findVarargMethod(Class<?> clz, String methodName, Class<?>[] parameterTypes) {
    if (parameterTypes.length == 0) {
      return null;
    }
    for (Method method : clz.getMethods()) {
      if (method.getName().equals(methodName)) {
        if (Modifier.isStatic(method.getModifiers()) || method.isBridge()) {
          continue;
        }
        Class<?>[] methodParaTypes = method.getParameterTypes();
        if (parameterTypes.length < methodParaTypes.length) {
          continue;
        }
        boolean argMatch = false;
        if (parameterTypes.length == methodParaTypes.length) { // length equal, compare arg type
          for (int index = 0; index < parameterTypes.length; index++) {
            argMatch = methodParaTypes[index].isAssignableFrom(parameterTypes[index]);
            if (!argMatch) {
              break;
            }
          }
        }
        if (!argMatch) { // try to match using var arg
          int firstVarargIndex = methodParaTypes.length - 1;
          if (firstVarargIndex >= 0 && methodParaTypes[firstVarargIndex].isArray()) {
            Class varArgClass = methodParaTypes[firstVarargIndex].getComponentType();
            for (int index = 0; index < parameterTypes.length; index++) {
              if (index < firstVarargIndex) {
                argMatch = methodParaTypes[index].isAssignableFrom(parameterTypes[index]);
              } else {
                argMatch = varArgClass.isAssignableFrom(parameterTypes[index]);
              }
              if (!argMatch) {
                break;
              }
            }
          }
        }
        if (argMatch) {
          return method;
        }
      }
    }
    return null;
  }

  public static Object[] adaptVarargMethodDataIfNecessary(Method method, Object[] data) {
    Class<?>[] methodParaTypes = method.getParameterTypes();
    if (data.length >= methodParaTypes.length) {
      int firstVarargIndex = methodParaTypes.length - 1;
      if (firstVarargIndex >= 0 && methodParaTypes[firstVarargIndex].isArray()) {
        Class varArgClass = methodParaTypes[firstVarargIndex].getComponentType();
        int numOfVararg = data.length - firstVarargIndex;
        Object varArg = Array.newInstance(varArgClass, numOfVararg);
        for (int i = 0; i < numOfVararg; ++i) {
          Array.set(varArg, i, data[firstVarargIndex + i]);
        }
        Object[] varArgs = new Object[firstVarargIndex + 1];
        for (int i = 0; i < firstVarargIndex; ++i) {
          varArgs[i] = data[i];
        }
        varArgs[firstVarargIndex] = varArg;
        return varArgs;
      }
    }
    return data;
  }

}
