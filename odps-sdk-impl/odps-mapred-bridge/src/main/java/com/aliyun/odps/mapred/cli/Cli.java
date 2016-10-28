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

package com.aliyun.odps.mapred.cli;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.cli.ParseException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mapred.conf.SessionState;

public class Cli {

  public static void main(String[] args) {
    SessionState ss = SessionState.get();
    OptionParser parser = new OptionParser(ss);
    try {
      parser.parse(args);
    } catch (FileNotFoundException e) {
      System.err.println(e.getMessage());
      System.exit(-1);
    } catch (ClassNotFoundException e) {
      System.err.println("Class not found:" + e.getMessage());
      System.exit(-1);
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      parser.usage();
      System.exit(-1);
    } catch (OdpsException e) {
      System.err.println(e.getMessage());
      System.exit(-1);
    }

    try {
      Method main = parser.getMainClass().getMethod("main", String[].class);
      main.invoke(null, (Object) parser.getArguments());
    } catch (InvocationTargetException e) {
      Throwable t = e.getCause();
      if (t != null && t instanceof OdpsException) {
        OdpsException ex = (OdpsException) t;
        System.err.println(ex.getErrorCode() + ":" + ex.getMessage());
        System.exit(-1);
      } else {
        throw new RuntimeException("Unknown error", e);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
