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

package com.aliyun.odps.udf.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aliyun.odps.utils.StringUtils;

import com.aliyun.odps.udf.OdpsType;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.PreferWritable;
import com.aliyun.odps.udf.annotation.Resolve;

/**
 * UDF SDK中用于内部实现的功能，这部分不属于API，接口随时可能改变，不推荐使用。
 */
public class AnnotationParser {

  @SuppressWarnings("serial")
  public static class ParseError extends UDFException {

    public ParseError(Throwable e) {
      super(e);
    }

    public ParseError(String e) {
      super(e);
    }
  }

  public static class Prototype {

    private OdpsType[] arguments;
    private OdpsType[] returns;

    public OdpsType[] getArguments() {
      return arguments;
    }

    public String getArgumentsString() {
      return convert2String(arguments);
    }

    public void setArguments(OdpsType[] arguments) {
      this.arguments = arguments;
    }

    public OdpsType[] getReturns() {
      return returns;
    }

    public String getReturnsString() {
      return convert2String(returns);
    }

    public void setReturns(OdpsType[] returns) {
      this.returns = returns;
    }

    private String convert2String(OdpsType[] types) {
      String[] s = new String[types.length];
      for (int i = 0; i < types.length; i++) {
        s[i] = types[i].name().toLowerCase();
      }
      return StringUtils.join(s, ',');
    }
  }

  public static class ParseResult {

    public boolean isVariadic() {
      return variadic;
    }

    public void setVariadic(boolean isVariadic) {
      this.variadic = isVariadic;
    }

    public List<Prototype> getProtoTypes() {
      return prototypes;
    }

    public void setTp(List<Prototype> tp) {
      this.prototypes = tp;
    }

    public boolean isWriable() {
      return writable;
    }

    public void setWritable(boolean val) {
      this.writable = val;
    }

    private boolean writable = false;
    private boolean variadic = false;
    private List<Prototype> prototypes = new ArrayList<Prototype>();
  }

  public static ParseResult parse(Class<?> clz) throws ParseError {
    ParseResult result = new ParseResult();
    Resolve r = clz.getAnnotation(Resolve.class);
    if (r == null) {
      throw new ParseError("@Resolve annotation not found.");
    }
    String[] infos = r.value();
    for (String info : infos) {
      String errMsg = "@Resolve({\"" + info + "\"}) ";
      if (info.isEmpty()) {
        throw new ParseError(errMsg + "must not be empty string");
      }
      int pos = info.indexOf("->");
      String args = "";
      if (pos > 0) {
        args = info.substring(0, pos);
      } else if (pos < 0) {
        throw new ParseError(errMsg);
      }
      String rtypes;
      int tPos = info.indexOf("->", pos + 2);
      if (tPos >= 0) {
        throw new ParseError(errMsg + "contains not exactly one '->'");
      }
      rtypes = info.substring(pos + 2, info.length());
      Prototype proto = new Prototype();
      String[] tokens = getTypeInfos(args);
      if (tokens == null) {
        throw new ParseError(errMsg + "annotates wrong arguments '" + args
                             + "'");
      }
      proto.setArguments(createTypes(tokens));
      if (rtypes.isEmpty()) {
        throw new ParseError(errMsg + "annotates no output types '" + args
                             + "'");
      }
      tokens = getTypeInfos(rtypes);
      if (tokens == null) {
        throw new ParseError(errMsg + "annotates wrong output types '" + rtypes
                             + "'");
      }
      proto.setReturns(createTypes(tokens));
      result.getProtoTypes().add(proto);
    }
    PreferWritable wr = clz.getAnnotation(PreferWritable.class);
    result.setWritable(wr != null);
    return result;
  }

  private static Set<String> registeredTypes = new HashSet<String>();

  static {
    registeredTypes.add(OdpsType.BIGINT.name());
    registeredTypes.add(OdpsType.STRING.name());
    registeredTypes.add(OdpsType.DOUBLE.name());
    registeredTypes.add(OdpsType.BOOLEAN.name());
    registeredTypes.add(OdpsType.DECIMAL.name());
    registeredTypes.add(OdpsType.DATETIME.name());
  }

  private static String[] getTypeInfos(String sig) {
    if (sig.isEmpty()) {
      return new String[0];
    }
    String[] sigArray = StringUtils.splitPreserveAllTokens(sig.toUpperCase(),
                                                           ',');

    for (String type : sigArray) {
      if (!registeredTypes.contains(type)) {
        return null;
      }
    }
    return sigArray;
  }

  private static OdpsType[] createTypes(String[] tokens) {
    OdpsType[] r = new OdpsType[tokens.length];
    for (int i = 0; i < tokens.length; i++) {
      // XXX: This is really ugly
      r[i] = OdpsType.valueOf(tokens[i].toUpperCase());
    }
    return r;
  }

}
