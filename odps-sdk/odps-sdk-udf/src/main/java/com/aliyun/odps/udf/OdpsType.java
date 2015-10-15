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

package com.aliyun.odps.udf;

/**
 * 映射到ODPS数据类型，包括
 * <ul>
 * <li>STRING：字符串</li>
 * <li>BIGINT：长整数型</li>
 * <li>DOUBLE：双精度符点数类型</li>
 * <li>BOOLEAN：双精度符点数类型</li>
 * <li>IGNORE：忽略类型映射</li>
 * </ul>
 * 不推荐直接使用。
 */
public enum OdpsType {
  STRING,
  BIGINT,
  DOUBLE,
  BOOLEAN,
  DATETIME,
  DECIMAL,
  
  @Deprecated
  IGNORE;
}
