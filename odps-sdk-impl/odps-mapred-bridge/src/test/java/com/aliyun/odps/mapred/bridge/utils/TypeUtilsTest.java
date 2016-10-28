package com.aliyun.odps.mapred.bridge.utils;


import apsara.odps.TypesProtos;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TypeUtilsTest {

  @Test
  public void testPrimitiveType() {
    Column col = new Column("col", OdpsType.STRING);
    Assert.assertEquals(TypesProtos.Type.String, TypeUtils.getLotTypeFromColumn(col));
  }

  @Test
  public void testMapType() {
    Column col = new Column("col", OdpsType.ARRAY);
    col.setGenericTypeList(Arrays.asList(new OdpsType[]{OdpsType.DOUBLE}));
    Assert.assertEquals(TypesProtos.Type.ArrayDouble, TypeUtils.getLotTypeFromColumn(col));
  }

  @Test
  public void testArrayType() {
    Column col = new Column("col", OdpsType.MAP);
    col.setGenericTypeList(Arrays.asList(new OdpsType[]{OdpsType.STRING, OdpsType.BIGINT}));
    Assert.assertEquals(TypesProtos.Type.MapStringInteger, TypeUtils.getLotTypeFromColumn(col));
  }
}
