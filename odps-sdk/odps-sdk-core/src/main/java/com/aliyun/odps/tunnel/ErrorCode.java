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

package com.aliyun.odps.tunnel;

/**
 * DataTunnel定义的错误代码。
 */
public interface ErrorCode {

  /**
   * 拒绝访问。
   */
  static final String ACCESS_DENIED = "AccessDenied";

  /**
   * 参数格式错误。
   */
  static final String INVALID_ARGUMENT = "InvalidArgument";

  /**
   * Access ID不存在。
   */
  static final String INVALID_ACCESS_KEY_ID = "InvalidAccessKeyId";

  /**
   * 无效的 Block ID。
   */
  static final String INVALID_BLOCK_ID = "InvalidBlockId";

  /**
   * 无效的 URI。
   */
  static final String INVALID_URI = "InvalidURI";

  /**
   * 无效的 RowRange。
   */
  static final String INVALID_ROWRANGE = "InvalidRowRange";

  /**
   * 无效的 Partition描述。
   */
  static final String INVALID_PARTITION_SPEC = "InvalidPartitionSpec";

  /**
   * Configuration 内部发生错误。
   */
  static final String INTERNAL_ERROR = "InternalError";

  /**
   * 缺少内容长度。
   */
  static final String MISSING_CONTENT_LENGTH = "MissingContentLength";

  /**
   * Table 不存在。
   */
  static final String NO_SUCH_TABLE = "NoSuchTable";

  /**
   * 无法处理的方法。
   */
  static final String NOT_IMPLEMENTED = "NotImplemented";

  /**
   * 预处理错误。
   */
  static final String PRECONDITION_FAILED = "PreconditionFailed";

  /**
   * 发起请求的时间和服务器时间超出15分钟。
   */
  static final String REQUEST_TIME_TOO_SKEWED = "RequestTimeTooSkewed";

  /**
   * 请求超时。
   */
  static final String REQUEST_TIMEOUT = "RequestTimeout";

  /**
   * 签名错误。
   */
  static final String SIGNATURE_DOES_NOT_MATCH = "SignatureDoesNotMatch";
}
