package com.aliyun.odps.data;

import org.junit.Assert;
import org.junit.Test;

public class JsonValueTest {

  @Test
  public void test() {
    JsonValue simpleJson = new SimpleJsonValue("123");
    Assert.assertEquals(123, simpleJson.getAsNumber().intValue());

    simpleJson = new SimpleJsonValue("null");
    Assert.assertTrue(simpleJson.isJsonNull());
    Assert.assertEquals("null", simpleJson.toString());

    simpleJson = new SimpleJsonValue("true");
    Assert.assertTrue(simpleJson.getAsBoolean());

    simpleJson = new SimpleJsonValue("{\"id\":123,\"name\":\"MaxCompute\"}");
    Assert.assertTrue(simpleJson.isJsonObject());
    JsonValue jsonObj = simpleJson.get("id");
    Assert.assertEquals(123, jsonObj.getAsNumber().intValue());
    Assert.assertEquals("MaxCompute", simpleJson.get("name").getAsString());

    simpleJson = new SimpleJsonValue("{\"id\":123,\"id\":345}");
    Assert.assertTrue(simpleJson.isJsonObject());
    jsonObj = simpleJson.get("id");
    Assert.assertEquals(345, jsonObj.getAsNumber().intValue());

    simpleJson = new SimpleJsonValue("[12, 34]");
    Assert.assertTrue(simpleJson.isJsonArray());
    jsonObj = simpleJson.get(1);
    Assert.assertEquals(34, jsonObj.getAsNumber().intValue());
    Assert.assertEquals(2, simpleJson.size());

    simpleJson = new SimpleJsonValue("null");
    Assert.assertTrue(simpleJson.isJsonNull());

    simpleJson = new SimpleJsonValue("\"null\"");
    Assert.assertFalse(simpleJson.isJsonNull());
  }

  @Test
  public void testException() {
    try {
      new SimpleJsonValue("{\"id\",123}}");
      Assert.fail("IllegalArgumentException not thrown");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    try {
      JsonValue simpleJson = new SimpleJsonValue("123");
      simpleJson.get(1);
      Assert.fail("IllegalStateException not thrown");
    } catch (UnsupportedOperationException e) {
      Assert.assertTrue(true);
    }

    try {
      JsonValue simpleJson = new SimpleJsonValue("{\"id\": 123}");
      simpleJson.size();
      Assert.fail("IllegalStateException not thrown");
    } catch (UnsupportedOperationException e) {
      Assert.assertTrue(true);
    }

    try {
      new SimpleJsonValue("hello");
      Assert.fail("IllegalArgumentException not thrown");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
  }

}
