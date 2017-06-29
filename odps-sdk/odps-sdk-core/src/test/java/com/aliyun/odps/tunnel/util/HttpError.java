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

package com.aliyun.odps.tunnel.util;

public class HttpError {

  public static final String
      RequestExpired =
      "Your socket connection to the server was not read from or written to within the expired period.";
  public static final String InvalidRowRange = "The specified row range is not valid.";
  public static final String InvalidBlockID = "The specified block id is not valid.";
  public static final String InvalidArgument = "Invalid argument.";
  public static final String InvalidURI = "Couldn't parse the specified URI.";
  public static final String
      MissingPartitionSpec =
      "You need to specify a partitionspec along with the specified table.";
  public static final String InvalidPartitionSpec = "The specified partitionspec is not valid.";
  public static final String
      IncompleteBody =
      "You did not provide the number of bytes specified by the Content-Length HTTP header.";
  public static final String MaxMessageLengthExceeded = "Your request was too big.";
  public static final String
      MalformedXML =
      "The XML you provided was not well-formed or did not validate against schema.";
  public static final String
      MalformedDataStream =
      "The data stream you provided was not well-formed or did not validate against schema.";
  public static final String MalformedHeaderValue = "An HTTP header value was malformed.";
  public static final String MissingRequestBodyError = "The request body is missing.";
  public static final String
      MissingRequiredHeaderError =
      "Your request was missing a required header.";
  public static final String UnexpectedContent = "This request does not support content.";
  public static final String
      Unauthorized =
      "The request authorization header is invalid or missing.";
  public static final String AccessDenied = "Access Denied.";
  public static final String
      StatusConflict =
      "You cannot complete the specified operation under the current upload or download status.";
  public static final String
      NoPermission =
      "You do not have enough privilege to complete the specified operation.";
  public static final String
      InvalidStatusChange =
      "You cannot change the specified upload or download status.";
  public static final String
      InConsistentBlockList =
      "The specified block list is not consistent with the uploaded block list on server side.";
  public static final String NoSuchTable = "The specified table name does not exist.";
  public static final String NoSuchPartition = "The specified partition does not exist.";
  public static final String NoSuchUpload = "The specified upload id does not exist.";
  public static final String NoSuchDownload = "The specified download id does not exist.";
  public static final String
      NoSuchData =
      "The uploaded data within this uploaded no longer exists.";
  public static final String
      MethodNotAllowed =
      "The specified method is not allowed against this resource.";
  public static final String
      TableModified =
      "The specified table has been modified since the download initiated. Try initiate another download.";
  public static final String
      MissingContentLength =
      "You must provide the Content-Length HTTP header.";
  public static final String
      ObjectModified =
      "The specified object has been modified since the specified timestamp.";
  public static final String
      InternalServerError =
      "Service internal error, please try again later.";
  public static final String
      NotImplemented =
      "A header you provided implies functionality that is not implemented.";
  public static final String
      ServiceUnavailable =
      "Service is temporarily unavailable, Please try again later.";
  public static final String NoSuchProject = "The specified project name does not exist.";
}
