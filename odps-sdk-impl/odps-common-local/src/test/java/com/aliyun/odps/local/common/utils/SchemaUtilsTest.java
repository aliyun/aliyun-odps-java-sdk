package com.aliyun.odps.local.common.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.local.common.ColumnOrConstant;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.TypeInfoParser;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class SchemaUtilsTest {

  @Test
  public void fromToString() {
    String columnStr = "c1:MAP<BIGINT,STRING>,c2:ARRAY<INT>,c3:STRUCT<name:CHAR(10),age:INT,"
      + "parents:MAP<VARCHAR(20),SMALLINT>,hehe:DECIMAL(20,10),salary:FLOAT,hobbies:ARRAY<VARCHAR(100)>>";
    Column[] cols = SchemaUtils.fromString(columnStr);
    Assert.assertTrue(cols.length == 3);
    Assert.assertTrue(cols[0].getTypeInfo() instanceof MapTypeInfo);
    MapTypeInfo mapTypeInfo = (MapTypeInfo)cols[0].getTypeInfo();
    Assert.assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT), mapTypeInfo.getKeyTypeInfo());
    Assert.assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING), mapTypeInfo.getValueTypeInfo());
    Assert.assertTrue(cols[1].getTypeInfo() instanceof ArrayTypeInfo);
    Assert.assertTrue(cols[2].getTypeInfo() instanceof StructTypeInfo);
    StructTypeInfo structTypeInfo = (StructTypeInfo) cols[2].getTypeInfo();
    List<TypeInfo> typeInfos = structTypeInfo.getFieldTypeInfos();
    Assert.assertTrue(typeInfos.size() == 6);
    Assert.assertTrue(typeInfos.get(0) instanceof CharTypeInfo);

    String str = SchemaUtils.toString(cols);
    Assert.assertEquals(columnStr, str);
  }

  @Test
  public void parseResolveTypeInfo() {
    List<TypeInfo> typeInfos = SchemaUtils.parseResolveTypeInfo("string,array<bigint>");
    Assert.assertTrue(typeInfos.size() == 2);
    Assert.assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING), typeInfos.get(0));
    Assert.assertTrue(typeInfos.get(1) instanceof ArrayTypeInfo);
    Assert.assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT), ((ArrayTypeInfo)typeInfos.get(1)).getElementTypeInfo());

    typeInfos = SchemaUtils.parseResolveTypeInfo("map<string,bigint>,struct<b1:bigint, b2:binary>");
    Assert.assertTrue(typeInfos.size() == 2);
    Assert.assertTrue(typeInfos.get(0) instanceof MapTypeInfo);
    MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfos.get(0);
    Assert.assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING), mapTypeInfo.getKeyTypeInfo());
    Assert.assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT), mapTypeInfo.getValueTypeInfo());
    Assert.assertTrue(typeInfos.get(1) instanceof StructTypeInfo);
    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfos.get(1);
    typeInfos = structTypeInfo.getFieldTypeInfos();
    Assert.assertTrue(typeInfos.size() == 2);
    Assert.assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT), typeInfos.get(0));
    Assert.assertEquals(TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BINARY), typeInfos.get(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseStarResolveTypeInfo() {
    SchemaUtils.parseResolveTypeInfo("string,*");
  }

  @Test(expected = IllegalArgumentException.class)
  public void parseInvalidResolveTypeInfo() {
    SchemaUtils.parseResolveTypeInfo("str,string");
  }

  @Test
  public void splitAndParseColumn() {
    String columnStr = "abc ,\",\",\":\", 1 | bigint";
    String[] columns = SchemaUtils.splitColumn(columnStr);
    Assert.assertEquals(4, columns.length);

    Assert.assertEquals("abc", columns[0]);
    ColumnOrConstant columnOrConstant = SchemaUtils.parseColumn(columns[0], null);
    Assert.assertFalse(columnOrConstant.isConstant());
    Assert.assertEquals("abc", columnOrConstant.getColName());

    Assert.assertEquals("\",\"", columns[1]);
    columnOrConstant = SchemaUtils.parseColumn(columns[1], null);
    Assert.assertTrue(columnOrConstant.isConstant());
    Assert.assertEquals(",", columnOrConstant.getConstantValue());
    Assert.assertEquals(TypeInfoParser.getTypeInfoFromTypeString("string"), columnOrConstant.getConstantTypeInfo());

    Assert.assertEquals("\":\"", columns[2]);
    columnOrConstant = SchemaUtils.parseColumn(columns[2], null);
    Assert.assertTrue(columnOrConstant.isConstant());
    Assert.assertEquals(":", columnOrConstant.getConstantValue());
    Assert.assertEquals(TypeInfoParser.getTypeInfoFromTypeString("string"), columnOrConstant.getConstantTypeInfo());

    Assert.assertEquals("1 | bigint", columns[3]);
    columnOrConstant = SchemaUtils.parseColumn(columns[3], null);
    Assert.assertTrue(columnOrConstant.isConstant());
    Assert.assertEquals(Long.parseLong("1"), columnOrConstant.getConstantValue());
    Assert.assertEquals(TypeInfoParser.getTypeInfoFromTypeString("bigint"), columnOrConstant.getConstantTypeInfo());
  }

}
