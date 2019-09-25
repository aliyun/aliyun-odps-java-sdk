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

package com.aliyun.odps.task;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.Task;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
/**
 * XDebug 开发时，提交计算任务用
 *
 * @author yangxu
 */
@Root(name = "XLib", strict = false)
public class XLibTask extends Task {

  @Element(name = "Method", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  private String method;

  @Element(name = "Parameters", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  private String parameters;

  @Element(name = "InputTablenames", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  private String inputTablenames;

  @Element(name = "OutputTablenames", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  private String outputTablenames;

  // Package-visible. Only for JAXB to construct the instance.
  XLibTask() {
  }

  /**
   * 使用给定任务名构造一个{@link XDebugTask}实例。
   *
   * @param name
   *     任务名。
   */
  public XLibTask(String name) {
    this(name, null, null);
  }

  /**
   * 使用给定任务名和命令构造一个{@link XDebugTask}实例。
   *
   * @param name
   *     任务名。
   * @param method
   *     方法。
   * @param parameters
   *     参数。
   */
  public XLibTask(String name, String method, String parameters) {
    // super(name);
    setName(name);
    setMethod(method);
    setParameters(parameters);
  }

  /**
   * 返回方法名。
   *
   * @return 方法名。
   */
  public String getMethod() {
    return method;
  }

  /**
   * 设置方法名。
   *
   * @param method
   *     方法名。
   */
  public void setMethod(String method) {
    this.method = method;
  }

  /**
   * 返回参数
   *
   * @return 参数。
   */
  public String getParameters() {
    return parameters;
  }

  /**
   * 设置参数。
   *
   * @param parameters
   *     参数。
   */
  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  /**
   * @return the inputTablenames
   */
  public String getInputTablenames() {
    return inputTablenames;
  }

  /**
   * @param inputTablenames
   *     the inputTablenames to set
   */
  public void setInputTablenames(String inputTablenames) {
    this.inputTablenames = inputTablenames;
  }

  /**
   * @return the outputTablenames
   */
  public String getOutputTablenames() {
    return outputTablenames;
  }

  /**
   * @param outputTablenames
   *     the outputTablenames to set
   */
  public void setOutputTablenames(String outputTablenames) {
    this.outputTablenames = outputTablenames;
  }
}
