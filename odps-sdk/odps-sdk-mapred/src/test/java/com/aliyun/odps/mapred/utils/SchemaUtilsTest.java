package com.aliyun.odps.mapred.utils;


import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import org.junit.Assert;
import org.junit.Test;

public class SchemaUtilsTest {

  @Test
  public void testArrayTypeLoad() {
    Column[] schema = SchemaUtils.fromString("key:BIGINT,value:ARRAY<STRING>");
    Assert.assertEquals(2, schema.length);
    Assert.assertEquals(OdpsType.ARRAY, schema[1].getType());
    Assert.assertEquals(OdpsType.STRING, schema[1].getGenericTypeList().get(0));
  }

  @Test
  public void testMapTypeLoad() {
    Column[] schema = SchemaUtils.fromString("key:BIGINT,value:MAP<STRING,BIGINT>");
    Assert.assertEquals(2, schema.length);
    Assert.assertEquals(OdpsType.MAP, schema[1].getType());
    Assert.assertEquals(OdpsType.STRING, schema[1].getGenericTypeList().get(0));
    Assert.assertEquals(OdpsType.BIGINT, schema[1].getGenericTypeList().get(1));
  }

}
