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

import java.util.concurrent.ConcurrentHashMap;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class OdpsDeprecatedLogger {

  private static ConcurrentHashMap<String, Long>
      deprecatedCalls =
      new ConcurrentHashMap<String, Long>();

  @Around("@annotation(Deprecated)")
  public Object around(ProceedingJoinPoint point) throws Throwable {
    try {
      String methodSignature = point.getSignature().toString();
      Long calledTimes = getDeprecatedCalls().get(methodSignature);
      if (calledTimes == null) {
        calledTimes = 1L;
      } else {
        calledTimes += 1L;
      }
      getDeprecatedCalls().put(methodSignature, calledTimes);
    } catch (Throwable e) {
      //do nothing.
    }
    return point.proceed();
  }

  @Around("@annotation(Survey)")
  public Object aroundOdpsImpl(ProceedingJoinPoint point) throws Throwable {
    //Thread Class
    //Deprecated Logger
    //Odps Implemention methods
    //User Code

    // if usercode is not odps calling, log it.
    try {
      String callerClass = Thread.currentThread().getStackTrace()[3].getClassName();
      if (!callerClass.startsWith("com.aliyun.odps.")) {
        String methodSignature = point.getSignature().toString();
        Long calledTimes = getDeprecatedCalls().get(methodSignature);
        if (calledTimes == null) {
          calledTimes = 1L;
        } else {
          calledTimes += 1L;
        }
        getDeprecatedCalls().put(methodSignature, calledTimes);
      }
    } catch (Throwable e) {
      // do nothing
    }
    return point.proceed();
  }


  public static ConcurrentHashMap<String, Long> getDeprecatedCalls() {
    return deprecatedCalls;
  }

  public static void setDeprecatedCalls(ConcurrentHashMap<String, Long> deprecatedCalls) {
    OdpsDeprecatedLogger.deprecatedCalls = deprecatedCalls;
  }
}
