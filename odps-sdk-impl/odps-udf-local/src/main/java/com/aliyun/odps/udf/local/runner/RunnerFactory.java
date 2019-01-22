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
import org.apache.commons.cli.CommandLine;

import com.aliyun.odps.Odps;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.local.LocalRunError;
import com.aliyun.odps.udf.local.LocalRunException;
import com.aliyun.odps.udf.local.util.ClassUtils;

public class RunnerFactory {

  public static BaseRunner getRunner(CommandLine cmdl, Odps odps, TableInfo tableInfo) throws LocalRunException,
                                                                         UDFException {

    String className = cmdl.getOptionValue("c");

    Object obj = null;
    try {
      obj = ClassUtils.newInstance(RunnerFactory.class.getClassLoader(), className);
    } catch (Exception e) {
      throw new LocalRunError("Invalid class name:" + className + "\n" + e.getMessage());
    }

    if (obj instanceof UDF) {
      return new UDFRunner(odps, className, tableInfo);
    } else if (obj instanceof UDTF) {
      return new UDTFRunner(odps, className);
    } else if (obj instanceof Aggregator) {
      return new AggregatorRunner(odps, className);
    }

    throw new LocalRunError("Unsupported class:" + className
                            + ", it must extends from one of the following:\n"
                            + "com.aliyun.odps.udf.UDF\n"
                            + "com.aliyun.odps.udf.UDTF\n" + "com.aliyun.odps.udf.Aggregator\n");
  }

}
