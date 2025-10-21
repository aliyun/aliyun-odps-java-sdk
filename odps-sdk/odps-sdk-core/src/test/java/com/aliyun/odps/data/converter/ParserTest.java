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

package com.aliyun.odps.data.converter;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.type.TypeInfoFactory;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ParserTest {

  static OdpsRecordConverter parser;

  static {
    parser = OdpsRecordConverter.builder()
      .enableParseNull()
      .nullFormat("\\N")
      .binaryFormatUtf8()
      .timezone("Asia/Shanghai")
      .build();
  }

  @Test
  public void parseNullKeyMap() {
    String value = "{:NULL}";
    Object
      object =
      parser.parseObject(value, TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING,
                                                               TypeInfoFactory.STRING));
    Assert.assertEquals(HashMap.class, object.getClass());

    System.out.println(object);
    Assert.assertEquals(null, ((Map) object).get(""));
  }
}
