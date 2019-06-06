package com.aliyun.odps.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * @author haoxiang.mhx@alibaba-inc.com
 *
 * Test gson serialization, especially when dealing with Double/Long.
 */
public class GsonTest {

  public class Person {
    public String name;
    public int age;
    public Map<String, Object> remark;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }

    public Map<String, Object> getRemark() {
      return remark;
    }

    public void setRemark(Map<String, Object> remark) {
      this.remark = remark;
    }
  }

  public class Summary {
    public String taskName;
    public Map<String, Map<Object, Object>> remark;

    public String getTaskName() {
      return taskName;
    }

    public void setTaskName(String taskName) {
      this.taskName = taskName;
    }

    public Map<String, Map<Object, Object>> getRemark() {
      return remark;
    }

    public void setRemark(Map<String, Map<Object, Object>> remark) {
      this.remark = remark;
    }
  }

  public Gson gson = null;

  @Before
  public void setup() {
    gson = GsonObjectBuilder.get();
  }

  @Test
  public void testNumberSerialize() {
    String json1 = "{\"id\": \"123456\", \"age\": 20, \"score\": 99.999}";
    Map<String, Object> obj1 = gson.fromJson(json1, new TypeToken<Map<String, Object>>(){}.getType());
    Assert.assertTrue(obj1.get("id") instanceof String);
    Assert.assertTrue(obj1.get("age") instanceof Long);
    Assert.assertTrue(obj1.get("score") instanceof Double);
    Assert.assertEquals(obj1.get("id"), "123456");
    Assert.assertEquals(obj1.get("age"), 20L);
    Assert.assertEquals((Double)obj1.get("score"), 99.999, 1E5);

    String json2 = "{\"friends\": {\"ben\": \"10086\", \"jason\":\"10010\"}, \"colleagues\":{\"jacky\": 18, \"sam\": 38.888}}";
    Map<String, Map<String, Object>> obj2 = gson.fromJson(json2, new TypeToken<Map<String, Map<String, Object>>>(){}.getType());
    Assert.assertTrue(obj2.get("friends").get("ben") instanceof String);
    Assert.assertTrue(obj2.get("colleagues").get("jacky") instanceof Long);
    Assert.assertTrue(obj2.get("colleagues").get("sam") instanceof Double);
    Assert.assertEquals(obj2.get("friends").get("ben"), "10086");
    Assert.assertEquals(obj2.get("colleagues").get("jacky"), 18L);
    Assert.assertEquals((Double)obj2.get("colleagues").get("sam"), 38.888, 1E5);
  }

  @Test
  public void testPojoSerialize() {
    String json1 = "{\"name\": \"ben\", \"age\": 25, \"remark\": {\"hobby\": \"swimming\", \"height\": 180.3, \"weight\": 80}}";
    Person obj1 = gson.fromJson(json1, Person.class);
    Assert.assertTrue(obj1.getRemark().get("hobby") instanceof String);
    Assert.assertTrue(obj1.getRemark().get("height") instanceof Double);
    Assert.assertTrue(obj1.getRemark().get("weight") instanceof Long);
    Assert.assertEquals(obj1.getName(), "ben");
    Assert.assertEquals(obj1.getAge(), 25);
    Assert.assertEquals(obj1.getRemark().get("weight"), 80L);
    Assert.assertEquals((Double)obj1.getRemark().get("height"), 180.3, 1E5);

    String json2 = "{\"taskName\": \"test\", \"remark\": " +
            "{\"score\": {\"round1\": 100, \"round2\": 88.3}, \"comment\": {\"round1\": \"success\", \"round2\": \"fail\"}}}";
    Summary obj2 = gson.fromJson(json2, Summary.class);
    Assert.assertTrue(obj2.getRemark().get("score").get("round1") instanceof Long);
    Assert.assertTrue(obj2.getRemark().get("score").get("round2") instanceof Double);
    Assert.assertTrue(obj2.getRemark().get("comment").get("round1") instanceof String);
    Assert.assertEquals(obj2.getRemark().get("score").get("round1"), 100L);
    Assert.assertEquals((Double)obj2.getRemark().get("score").get("round2"), 88.3, 1E5);
  }
}
