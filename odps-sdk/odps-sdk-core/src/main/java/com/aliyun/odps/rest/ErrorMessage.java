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

package com.aliyun.odps.rest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.GsonObjectBuilder;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

/**
 * 表示ODPS RESTful API返回的出错信息
 */
@Root(name = "Error", strict = false)
public class ErrorMessage {

  @Element(name = "Code", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  @Expose()
  @SerializedName("Code")
  private String errorcode;

  @Element(name = "Message", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  @Expose
  @SerializedName("Message")
  private String message;

  @Element(name = "RequestId", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  @Expose
  @SerializedName("RequestId")
  private String requestId;

  @Element(name = "HostId", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  public String HostId;

  public String getErrorcode() {
    return errorcode;
  }

  public String getMessage() {
    return message;
  }

  public String getRequestId() {
    return requestId;
  }

  public String getHostId() {
    return HostId;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("RequestId=").append(requestId).append(',');
    sb.append("Code=").append(errorcode).append(',');
    sb.append("Message=").append(message);
    return sb.toString();
  }

  private static ErrorMessage fromJson(byte[] body) {
    try {
      return GsonObjectBuilder
          .get()
          .fromJson(new String(body, StandardCharsets.UTF_8), ErrorMessage.class);
    } catch (Exception ignore) {
      return null;
    }
  }

  private static ErrorMessage fromXml(byte[] body) {
    try {
      return SimpleXmlUtils.unmarshal(body, ErrorMessage.class);
    } catch (Exception ignore) {
      return null;
    }
  }

  public static ErrorMessage from(byte[] body) {
    if (body == null) {
      return null;
    }

    ErrorMessage ret;
    ret = fromXml(body);
    if (ret == null) {
      ret = fromJson(body);
    }

    return ret;
  }
}
