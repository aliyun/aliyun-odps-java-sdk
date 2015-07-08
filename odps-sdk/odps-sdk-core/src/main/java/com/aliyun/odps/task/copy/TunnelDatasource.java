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

package com.aliyun.odps.task.copy;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.commons.util.TrimmedStringXmlAdapter;

@XmlRootElement(name = "Tunnel")
@XmlType(name = "",
    propOrder = {"endPoint", "odpsEndPoint", "signature", "accountType"})
public class TunnelDatasource extends Datasource {

  private String endPoint;
  private String odpsEndPoint;
  private String signature;
  private String accountType;

  TunnelDatasource() {

  }

  public TunnelDatasource(Direction direction, String project, String table, String partition) {
    super(direction == Direction.IMPORT ? "Source" : "Destination", project, table, partition);
  }

  public String getEndPoint() {
    return endPoint;
  }

  @XmlElement(name = "EndPoint")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  public void setEndPoint(String endPoint) {
    this.endPoint = endPoint;
  }

  public String getSignature() {
    return signature;
  }

  @XmlElement(name = "Signature")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  public void setSignature(String signature) {
    this.signature = signature;
  }

  public String getAccountType() {
    return accountType;
  }

  @XmlElement(name = "SignatureType")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  public void setAccountType(String accountType) {
    this.accountType = accountType;
  }

  public String getOdpsEndPoint() {
    return odpsEndPoint;
  }

  @XmlElement(name = "OdpsEndPoint")
  @XmlJavaTypeAdapter(TrimmedStringXmlAdapter.class)
  public void setOdpsEndPoint(String endPoint) {
    this.odpsEndPoint = endPoint;
  }
}

