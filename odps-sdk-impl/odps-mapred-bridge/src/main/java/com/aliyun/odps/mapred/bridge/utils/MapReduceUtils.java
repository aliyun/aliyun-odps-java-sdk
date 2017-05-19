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

package com.aliyun.odps.mapred.bridge.utils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.TaskContext;
import com.aliyun.odps.utils.ReflectionUtils;

public class MapReduceUtils {

  /**
   * Get method from class <code>clz</code> whose name is specified as
   * <code>methodName</code> and arguments are specified as <code>args</code>. Note that
   * the function will traverse parent classes if the declared method is not
   * exist in current class.
   *
   * @param clz
   * @param methodName
   * @param args
   * @return the finding method, or null if non-exist.
   */
  static Method getOverriddenMethod(Class<?> clz, String methodName, Class<?>... args) {
    List<Method> methods = null;
    while (clz != null) {
      // find all methods with the same name
      methods = ReflectionUtils.findUserClassMethods(clz, methodName);
      for (Method m : methods) {
        if (m != null && isAssignable(m.getParameterTypes(), args)) {
          return m;
        }
      }
      clz = clz.getSuperclass();
    }
    return null;
  }

  /**
   * Check if is parameters are assignable.
   *
   * @param parameterTypes
   * @param args
   * @return
   */
  private static boolean isAssignable(Class<?>[] parameterTypes, Class<?>[] args) {
    if (parameterTypes == null || args == null || parameterTypes.length != args.length) {
      return false;
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      if (!args[i].isAssignableFrom(parameterTypes[i])) {
        return false;
      }
    }
    return true;
  }


  private static void handleException(InvocationTargetException e) throws IOException {
    if (e.getTargetException() instanceof IOException) {
      throw (IOException) e.getTargetException();
    } else if (e.getTargetException() instanceof Error) {
      throw (Error) e.getTargetException();
    } else if (e.getTargetException() instanceof RuntimeException) {
      throw (RuntimeException) e.getTargetException();
    } else {
      throw new RuntimeException(e.getTargetException());
    }
  }

  /**
   * Run mapper using reflection.
   *
   * @param clz
   * @param ctx
   * @throws IOException
   */
  public static void runMapper(Class<Mapper> clz, Mapper.TaskContext ctx) throws IOException {
    Mapper mapper = (Mapper) ReflectionUtils.newInstance(clz, ctx.getJobConf());
    try {
      // should add a flag to this for compatibility
      if (ctx.getJobConf().getBoolean("odps.mapred.run.interface.enable", false)) {
        Method m = getOverriddenMethod(clz, "run", TaskContext.class);
        if (m != null) {
          m.invoke(mapper, ctx);
          return;
        }
      }
      Method m = getOverriddenMethod(clz, "setup", TaskContext.class);
      if (m != null) {
        m.invoke(mapper, ctx);
      }
      m = getOverriddenMethod(clz, "map", long.class, Record.class, TaskContext.class);
      if (m != null) {
        while (ctx.nextRecord()) {
          m.invoke(mapper, ctx.getCurrentRecordNum(), ctx.getCurrentRecord(), ctx);
        }
      }

      m = getOverriddenMethod(clz, "cleanup", TaskContext.class);
      if (m != null) {
        m.invoke(mapper, ctx);
      }
    } catch (InvocationTargetException e) {
      handleException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Run combiner/reducer using reflection
   *
   * @param clz
   * @param ctx
   * @throws IOException
   */
  public static void runReducer(Class<Reducer> clz, Reducer.TaskContext ctx) throws IOException {
    Reducer combiner = (Reducer) ReflectionUtils.newInstance(clz, ctx.getJobConf());
    try {
      // should add a flag to this for compatibility
      if (ctx.getJobConf().getBoolean("odps.mapred.run.interface.enable", false)) {
        Method m = getOverriddenMethod(clz, "run", TaskContext.class);
        if (m != null) {
          m.invoke(combiner, ctx);
          return;
        }
      }
      Method m = getOverriddenMethod(clz, "setup", TaskContext.class);
      if (m != null) {
        m.invoke(combiner, ctx);
      }
      m = getOverriddenMethod(clz, "reduce", Record.class, Iterator.class, TaskContext.class);
      if (m != null) {
        while (ctx.nextKeyValue()) {
          m.invoke(combiner, ctx.getCurrentKey(), ctx.getValues(), ctx);
        }
      }
      m = getOverriddenMethod(clz, "cleanup", TaskContext.class);
      if (m != null) {
        m.invoke(combiner, ctx);
      }
    } catch (InvocationTargetException e) {
      handleException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Check if TableInfo existing is semantically included by desc.
   *
   * @param desc
   * @param existing
   * @return
   */
  public static boolean partSpecInclusive(TableInfo desc, TableInfo existing) {
    if (!desc.getProjectName().equalsIgnoreCase(existing.getProjectName()) || !desc.getTableName()
        .equalsIgnoreCase(existing.getTableName())) {
      return false;
    }

    if (desc.getPartitionSpec() != null) {
      for (String key : desc.getPartSpec().keySet()) {
        if (!desc.getPartSpec().get(key).equals(existing.getPartSpec().get(key))) {
          return false;
        }
      }
    }
    return true;
  }

}
