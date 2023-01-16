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

package com.aliyun.odps;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Tag.TagModel;
import com.aliyun.odps.Tag.TagModelDeserializer;
import com.aliyun.odps.Tag.TagModelSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TagTest extends TestBase {
  @Test
  public void testTagModelSerializer() {
    Gson gson = new GsonBuilder()
        .registerTypeHierarchyAdapter(TagModel.class, new TagModelSerializer())
        .setPrettyPrinting()
        .create();

    TagModel model = new TagModel();
    model.project = "security_test_shuxu_2";
    model.classification = "test_classification";
    model.name = "test_tag_1";
    model.policy = "{\n    \"Statement\": [{\n            \"Action\": [\"odps:Download\",\n                \"odps:PurgeTable\"],\n            \"Effect\": \"Allow\",\n            \"Principal\": [\"ALIYUN$odpstest@aliyun.com\",\n                \"ALIYUN$odpstest1@aliyun.com\",\n                \"ALIYUN$odpstest2@aliyun.com\",\n                \"ALIYUN$odpstest3@aliyun.com\"],\n            \"Resource\": [\"acs:odps:*:projects/old_priv_prj/tables/de3_tbl_1\",\n                \"acs:odps:*:projects/old_priv_prj/tables/*\"]}],\n    \"Version\": \"1\"}";
    model.attributes.put("test_attr_1", "element-2");
    model.attributes.put("test_attr_2", "1000");
    model.attributes.put("test_attr_3", "阿里巴巴测试test");
    model.attributes.put("test_attr_4", "true");
    model.attributes.put("test_属性5", "test_属性5");

    String expected = "{\n"
        + "  \"DatabaseName\": \"security_test_shuxu_2\",\n"
        + "  \"Classification\": \"test_classification\",\n"
        + "  \"Name\": \"test_tag_1\",\n"
        + "  \"ExtendedInfo\": {\n"
        + "    \"Policy\": \"{\\n    \\\"Statement\\\": [{\\n            \\\"Action\\\": [\\\"odps:Download\\\",\\n                \\\"odps:PurgeTable\\\"],\\n            \\\"Effect\\\": \\\"Allow\\\",\\n            \\\"Principal\\\": [\\\"ALIYUN$odpstest@aliyun.com\\\",\\n                \\\"ALIYUN$odpstest1@aliyun.com\\\",\\n                \\\"ALIYUN$odpstest2@aliyun.com\\\",\\n                \\\"ALIYUN$odpstest3@aliyun.com\\\"],\\n            \\\"Resource\\\": [\\\"acs:odps:*:projects/old_priv_prj/tables/de3_tbl_1\\\",\\n                \\\"acs:odps:*:projects/old_priv_prj/tables/*\\\"]}],\\n    \\\"Version\\\": \\\"1\\\"}\"\n"
        + "  },\n"
        + "  \"TagValues\": {\n"
        + "    \"test_属性5\": \"test_属性5\",\n"
        + "    \"test_attr_4\": \"true\",\n"
        + "    \"test_attr_3\": \"阿里巴巴测试test\",\n"
        + "    \"test_attr_2\": \"1000\",\n"
        + "    \"test_attr_1\": \"element-2\"\n"
        + "  }\n"
        + "}";
    Assert.assertEquals(expected, gson.toJson(model));
  }

  @Test
  public void testTagModelDeserializer() {
    Gson gson = new GsonBuilder()
        .registerTypeHierarchyAdapter(TagModel.class, new TagModelDeserializer())
        .create();

    String json = "{\"Classification\":\"test_classification\",\"CreateTime\":1606727313,\"DatabaseName\":\"security_test_shuxu_2\",\"ExtendedInfo\":{\"Policy\":\"{\\\"Statement\\\":[{\\\"Action\\\":[\\\"odps:Download\\\",\\\"odps:PurgeTable\\\"],\\\"Effect\\\":\\\"Allow\\\",\\\"Principal\\\":[\\\"ALIYUN$odpstest@aliyun.com\\\",\\\"ALIYUN$odpstest1@aliyun.com\\\",\\\"ALIYUN$odpstest2@aliyun.com\\\",\\\"ALIYUN$odpstest3@aliyun.com\\\"],\\\"Resource\\\":[\\\"acs:odps:*:projects/old_priv_prj/tables/de3_tbl_1\\\",\\\"acs:odps:*:projects/old_priv_prj/tables/*\\\"]}],\\\"Version\\\":\\\"1\\\"}\"},\"Id\":\"9df2441327a945f285268f6a2d01caf1\",\"Name\":\"test_tag_1\",\"Owner\":\"ALIYUN$odpstest4@aliyun.com\",\"TagValues\":{\"test_attr_1\":\"element-2\",\"test_attr_2\":\"1000\",\"test_attr_3\":\"阿里巴巴测试test\",\"test_attr_4\":\"true\",\"test_属性5\":\"安全部隐私test\"},\"UpdateTime\":1606796549}";

    TagModel model = gson.fromJson(json, TagModel.class);
    Assert.assertEquals("security_test_shuxu_2", model.project);
    Assert.assertEquals("test_classification", model.classification);
    Assert.assertEquals("test_tag_1", model.name);
    Assert.assertEquals("ALIYUN$odpstest4@aliyun.com", model.owner);
    Assert.assertEquals(1606727313, model.createdTime.getTime());
    Assert.assertEquals(1606796549, model.lastModifiedTime.getTime());
    String policy = "{\"Statement\":[{\"Action\":[\"odps:Download\",\"odps:PurgeTable\"],\"Effect\":\"Allow\",\"Principal\":[\"ALIYUN$odpstest@aliyun.com\",\"ALIYUN$odpstest1@aliyun.com\",\"ALIYUN$odpstest2@aliyun.com\",\"ALIYUN$odpstest3@aliyun.com\"],\"Resource\":[\"acs:odps:*:projects/old_priv_prj/tables/de3_tbl_1\",\"acs:odps:*:projects/old_priv_prj/tables/*\"]}],\"Version\":\"1\"}";
    Assert.assertEquals(policy, model.policy);

    Assert.assertEquals("element-2", model.attributes.get("test_attr_1"));
    Assert.assertEquals("1000", model.attributes.get("test_attr_2"));
    Assert.assertEquals("阿里巴巴测试test", model.attributes.get("test_attr_3"));
    Assert.assertEquals("true", model.attributes.get("test_attr_4"));
    Assert.assertEquals("安全部隐私test", model.attributes.get("test_属性5"));
  }
}
