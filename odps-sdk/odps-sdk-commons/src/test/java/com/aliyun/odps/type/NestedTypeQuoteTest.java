package com.aliyun.odps.type;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class NestedTypeQuoteTest {

  private static StructTypeInfo nestedTypeInfo;

  static {
    List<String> names = new ArrayList<>();
    names.add("end");
    names.add("start");
    List<TypeInfo> typeInfos = new ArrayList<>();
    typeInfos.add(TypeInfoFactory.STRING);
    typeInfos.add(TypeInfoFactory.BIGINT);

    nestedTypeInfo = TypeInfoFactory.getStructTypeInfo(names, typeInfos);
  }

  @Test
  public void testBasic() {
    String typeWithNoQuote = nestedTypeInfo.getTypeName();
    Assert.assertEquals("STRUCT<end:STRING,start:BIGINT>", typeWithNoQuote);
    String typeWithQuote = nestedTypeInfo.getTypeName(true);
    Assert.assertEquals("STRUCT<`end`:STRING,`start`:BIGINT>", typeWithQuote);
  }

  @Test
  public void testArray() {
    ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(nestedTypeInfo);
    String typeName = arrayTypeInfo.getTypeName();
    Assert.assertEquals("ARRAY<STRUCT<end:STRING,start:BIGINT>>", typeName);
    String typeNameWithQuote = arrayTypeInfo.getTypeName(true);
    Assert.assertEquals("ARRAY<STRUCT<`end`:STRING,`start`:BIGINT>>", typeNameWithQuote);
  }

  @Test
  public void testMap() {
    MapTypeInfo mapTypeInfo = TypeInfoFactory.getMapTypeInfo(nestedTypeInfo, nestedTypeInfo);
    String typeName = mapTypeInfo.getTypeName();
    Assert.assertEquals("MAP<STRUCT<end:STRING,start:BIGINT>,STRUCT<end:STRING,start:BIGINT>>", typeName);
    String typeNameWithQuote = mapTypeInfo.getTypeName(true);
    Assert.assertEquals("MAP<STRUCT<`end`:STRING,`start`:BIGINT>,STRUCT<`end`:STRING,`start`:BIGINT>>", typeNameWithQuote);
  }

  @Test
  public void testStruct() {
    List<String> names = new ArrayList<>();
    names.add("end");
    names.add("start");
    List<TypeInfo> typeInfos = new ArrayList<>();
    typeInfos.add(nestedTypeInfo);
    typeInfos.add(nestedTypeInfo);
    StructTypeInfo structTypeInfo = TypeInfoFactory.getStructTypeInfo(names, typeInfos);
    String typeName = structTypeInfo.getTypeName();
    Assert.assertEquals("STRUCT<end:STRUCT<end:STRING,start:BIGINT>,start:STRUCT<end:STRING,start:BIGINT>>", typeName);

    String typeNameWithQuote = structTypeInfo.getTypeName(true);
    Assert.assertEquals("STRUCT<`end`:STRUCT<`end`:STRING,`start`:BIGINT>,`start`:STRUCT<`end`:STRING,`start`:BIGINT>>", typeNameWithQuote);
  }

}
