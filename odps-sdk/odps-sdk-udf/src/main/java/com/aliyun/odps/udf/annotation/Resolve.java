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

package com.aliyun.odps.udf.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 用于指定{@link com.aliyun.odps.udf.UDTF}的输入输出类型。
 * <p>分别用于指定 {@link com.aliyun.odps.udf.UDTF#process(Object[])} 和 {@link
 * com.aliyun.odps.udf.UDTF#forward(Object[])} 的类型，
 * 语法如下</p>
 * <pre>
 * {@literal @}Resolve({ signature })
 *
 * signature: arg_type_list '->' type_list
 * arg_type_list: type_list | ''
 * type_list: [type_list ','] type
 * type: 'bigint' | 'string' | 'double' | 'boolean'
 * </pre>
 * 比如：
 * <pre>
 * {@literal @}Resolve({"->string"}) // 参数为空，输出一列string字段
 * {@literal @}Resolve({"bigint,boolean->string,double"}) // 参数为bigint,boolean，输出string,double字段
 * </pre>
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Resolve {

  /**
   * 目前只支持数组长度为1的value值。
   */
  String[] value();
}
