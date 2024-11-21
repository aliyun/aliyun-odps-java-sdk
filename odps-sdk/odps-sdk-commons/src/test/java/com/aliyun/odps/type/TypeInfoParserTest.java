package com.aliyun.odps.type;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsType;


/**
 * Created by zhenhong.gzh on 16/8/10.
 */
public class TypeInfoParserTest {

  @Test
  public void testParsrPrimitiveTypes() {
    final String[] tinyints = {"tinyint", "TINYINT", "TiNYInt"};
    final String[] smallints = {"smallint", "SMALLINT", "SmallINT"};
    final String[] ints = {"int", "INT", "Int"};
    final String[] bigints = {"bigint", "BIGINT", "BigInt"};
    final String[] doubles = {"double", "DOUBLE", "doubLE"};
    final String[] booleans = {"boolean", "BOOLEAN", "BoolEAN"};
    final String[] datetimes = {"datetime", "DATETIME", "DAteTime"};
    final String[] dates = {"DATE", "date", "DAtE"};
    final String[] strings = {"string", "STRING", "STRing"};
    final String[] decimals = {"DECIMAL", "decimal", "DEcimal"};
    final String[] voids = {"VOID", "void", "VOid"};
    final String[] floats = {"float", "FLOAT", "FloaT"};
    final String[] timestamps = {"timestamp", "TIMESTAMP", "TimeSTAMp"};
    final String[] binarys = {"binary", "BINARY", "BinaRY"};
    final String[] interval_days = {"interval_day_time", "INTERVAL_DAY_TIME", "InterVal_day_TIME"};
    final String[]
        interval_years =
        {"interval_year_month", "INTERVAL_YEAR_MONTH", "inTErval_Year_moNTh"};

    testPrimitve(tinyints, OdpsType.TINYINT);
    testPrimitve(smallints, OdpsType.SMALLINT);
    testPrimitve(ints, OdpsType.INT);
    testPrimitve(bigints, OdpsType.BIGINT);
    testPrimitve(doubles, OdpsType.DOUBLE);
    testPrimitve(booleans, OdpsType.BOOLEAN);
    testPrimitve(datetimes, OdpsType.DATETIME);
    testPrimitve(dates, OdpsType.DATE);
    testPrimitve(strings, OdpsType.STRING);
    testPrimitve(decimals, OdpsType.DECIMAL);
    testPrimitve(voids, OdpsType.VOID);
    testPrimitve(floats, OdpsType.FLOAT);
    testPrimitve(timestamps, OdpsType.TIMESTAMP);
    testPrimitve(binarys, OdpsType.BINARY);
    testPrimitve(interval_days, OdpsType.INTERVAL_DAY_TIME);
    testPrimitve(interval_years, OdpsType.INTERVAL_YEAR_MONTH);
  }

  public void testPrimitve(String[] names, OdpsType type) {
    for (String name : names) {
      TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
      Assert.assertEquals(typeInfo.getOdpsType(), type);
    }
  }

  @Test
  public void testParseArray() {
    String array = "array<int>";

    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(array);
    Assert.assertTrue(typeInfo instanceof ArrayTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), array.toUpperCase());
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.ARRAY);
    Assert.assertEquals(((ArrayTypeInfo) typeInfo).getElementTypeInfo(), TypeInfoFactory.INT);

    array = "array<BigInt>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(array);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.ARRAY);
    Assert.assertTrue(typeInfo instanceof ArrayTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), array.toUpperCase());
    Assert.assertEquals(((ArrayTypeInfo) typeInfo).getElementTypeInfo(), TypeInfoFactory.BIGINT);

    array = "ARRAY<map<String,Decimal(10,2)>>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(array);
    Assert.assertTrue(typeInfo instanceof ArrayTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), array.toUpperCase());
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.ARRAY);
    Assert.assertEquals(((ArrayTypeInfo) typeInfo).getElementTypeInfo().getTypeName(),
                        "MAP<STRING,DECIMAL(10,2)>");
    TypeInfo element = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
    Assert.assertTrue(element instanceof MapTypeInfo);
    Assert.assertEquals(element.getOdpsType(), OdpsType.MAP);
    Assert.assertEquals(((MapTypeInfo) element).getKeyTypeInfo(), TypeInfoFactory.STRING);
    Assert.assertEquals(((MapTypeInfo) element).getValueTypeInfo().getOdpsType(), OdpsType.DECIMAL);
    Assert.assertEquals(((MapTypeInfo) element).getValueTypeInfo().getTypeName(), "DECIMAL(10,2)");
    Assert
        .assertEquals(((DecimalTypeInfo) ((MapTypeInfo) element).getValueTypeInfo()).getPrecision(),
                      10);
    Assert
        .assertEquals(((DecimalTypeInfo) ((MapTypeInfo) element).getValueTypeInfo()).getScale(), 2);

    array = "ARRay<VARchar(10)>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(array);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.ARRAY);
    Assert.assertTrue(typeInfo instanceof ArrayTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), array.toUpperCase());
    Assert.assertEquals(((ArrayTypeInfo) typeInfo).getElementTypeInfo().getOdpsType(),
                        OdpsType.VARCHAR);
    Assert
        .assertEquals(((ArrayTypeInfo) typeInfo).getElementTypeInfo().getTypeName(), "VARCHAR(10)");
    element = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
    Assert.assertEquals(((VarcharTypeInfo) element).getLength(), 10);
  }

  @Test
  public void testParseDecimal() {
    String decimal = "DECIMAL(19,4)";

    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(decimal);
    Assert.assertTrue(typeInfo instanceof DecimalTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), decimal.toUpperCase());
    Assert.assertEquals(((DecimalTypeInfo) typeInfo).getPrecision(), 19);
    Assert.assertEquals(((DecimalTypeInfo) typeInfo).getScale(), 4);

    decimal = "decimal(3,2)";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(decimal);
    Assert.assertTrue(typeInfo instanceof DecimalTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), decimal.toUpperCase());
    Assert.assertEquals(((DecimalTypeInfo) typeInfo).getPrecision(), 3);
    Assert.assertEquals(((DecimalTypeInfo) typeInfo).getScale(), 2);

    decimal = "DecimaL(5,0)";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(decimal);
    Assert.assertTrue(typeInfo instanceof DecimalTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), decimal.toUpperCase());
    Assert.assertEquals(((DecimalTypeInfo) typeInfo).getPrecision(), 5);
    Assert.assertEquals(((DecimalTypeInfo) typeInfo).getScale(), 0);
  }

  @Test
  public void testParseChar() {
    String name = "char(10)";
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof CharTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.CHAR);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((CharTypeInfo) typeInfo).getLength(), 10);

    name = "CHAR(20)";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof CharTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.CHAR);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((CharTypeInfo) typeInfo).getLength(), 20);

    name = "ChaR(1)";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof CharTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.CHAR);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((CharTypeInfo) typeInfo).getLength(), 1);
  }

  @Test
  public void testParseVarchar() {
    String name = "varchar(10)";
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof VarcharTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.VARCHAR);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((VarcharTypeInfo) typeInfo).getLength(), 10);

    name = "VARCHAR(20)";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof VarcharTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.VARCHAR);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((VarcharTypeInfo) typeInfo).getLength(), 20);

    name = "vArChaR(1)";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof VarcharTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.VARCHAR);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((VarcharTypeInfo) typeInfo).getLength(), 1);
  }

  @Test
  public void testParseMap() {
    String name = "map<int,bigint>";
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof MapTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.MAP);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((MapTypeInfo) typeInfo).getKeyTypeInfo(), TypeInfoFactory.INT);
    Assert.assertEquals(((MapTypeInfo) typeInfo).getValueTypeInfo(), TypeInfoFactory.BIGINT);

    name = "MAP<String,Array<int>>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof MapTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.MAP);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((MapTypeInfo) typeInfo).getKeyTypeInfo(), TypeInfoFactory.STRING);
    Assert.assertTrue(((MapTypeInfo) typeInfo).getValueTypeInfo() instanceof ArrayTypeInfo);
    Assert.assertEquals(((MapTypeInfo) typeInfo).getValueTypeInfo().getOdpsType(), OdpsType.ARRAY);
    Assert.assertEquals(
        ((ArrayTypeInfo) (((MapTypeInfo) typeInfo).getValueTypeInfo())).getElementTypeInfo(),
        TypeInfoFactory.INT);

    name = "MAP<DECIMAL(5,2),DECIMAL>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);

    Assert.assertTrue(typeInfo instanceof MapTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.MAP);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertTrue(((MapTypeInfo) typeInfo).getKeyTypeInfo() instanceof DecimalTypeInfo);
    Assert.assertEquals(((MapTypeInfo) typeInfo).getKeyTypeInfo().getOdpsType(), OdpsType.DECIMAL);
    Assert.assertEquals(
        ((DecimalTypeInfo) (((MapTypeInfo) typeInfo).getKeyTypeInfo())).getPrecision(), 5);
    Assert.assertEquals(
        ((DecimalTypeInfo) (((MapTypeInfo) typeInfo).getKeyTypeInfo())).getScale(), 2);
    Assert.assertEquals(((MapTypeInfo) typeInfo).getValueTypeInfo(), TypeInfoFactory.DECIMAL);

    name = "map<varchar(10),int>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);

    Assert.assertTrue(typeInfo instanceof MapTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.MAP);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertTrue(((MapTypeInfo) typeInfo).getKeyTypeInfo() instanceof VarcharTypeInfo);
    Assert.assertEquals(((MapTypeInfo) typeInfo).getKeyTypeInfo().getOdpsType(), OdpsType.VARCHAR);
    Assert.assertEquals(
        ((VarcharTypeInfo) (((MapTypeInfo) typeInfo).getKeyTypeInfo())).getLength(), 10);
    Assert.assertEquals(((MapTypeInfo) typeInfo).getValueTypeInfo(), TypeInfoFactory.INT);

    name = "map<tinyint,char(2)>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);

    Assert.assertTrue(typeInfo instanceof MapTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.MAP);
    Assert.assertEquals(typeInfo.getTypeName(), name.toUpperCase());
    Assert.assertEquals(((MapTypeInfo) typeInfo).getKeyTypeInfo(), TypeInfoFactory.TINYINT);
    Assert.assertEquals(((MapTypeInfo) typeInfo).getValueTypeInfo().getOdpsType(), OdpsType.CHAR);
    Assert.assertTrue(((MapTypeInfo) typeInfo).getValueTypeInfo() instanceof CharTypeInfo);

    Assert.assertEquals(
        ((CharTypeInfo) (((MapTypeInfo) typeInfo).getValueTypeInfo())).getLength(), 2);

    name = "map<decimal,struct<name:String,age:int>>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);

    Assert.assertTrue(typeInfo instanceof MapTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.MAP);
    Assert.assertEquals(typeInfo.getTypeName(), "MAP<DECIMAL,STRUCT<`name`:STRING,`age`:INT>>");
    Assert.assertEquals(((MapTypeInfo) typeInfo).getKeyTypeInfo(), TypeInfoFactory.DECIMAL);
    Assert.assertEquals(((MapTypeInfo) typeInfo).getValueTypeInfo().getOdpsType(), OdpsType.STRUCT);
    Assert.assertTrue(((MapTypeInfo) typeInfo).getValueTypeInfo() instanceof StructTypeInfo);

    Assert.assertArrayEquals(
        ((StructTypeInfo) (((MapTypeInfo) typeInfo).getValueTypeInfo())).getFieldNames().toArray(),
        new String[]{"name", "age"});

    Assert.assertArrayEquals(
        ((StructTypeInfo) (((MapTypeInfo) typeInfo).getValueTypeInfo())).getFieldTypeInfos()
            .toArray(), new TypeInfo[]{TypeInfoFactory.STRING, TypeInfoFactory.INT});

  }

  @Test
  public void testParseStruct() {

    String name1 =
        "struct<`name`:string,`age`:tinyint,`families`:map<string,int>,hobbies:array<string>,test:decimal(10,2),test2:decimal>";
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name1);

    validateStruct(typeInfo);

    String name = "strUCT<name:char(8),test:" + name1 + ">";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);

    Assert.assertEquals(((StructTypeInfo) typeInfo).getFieldNames().get(0), "name");
    Assert.assertEquals(((StructTypeInfo) typeInfo).getFieldNames().get(1), "test");

    Assert.assertTrue(
        ((StructTypeInfo) typeInfo).getFieldTypeInfos().get(0) instanceof CharTypeInfo);
    Assert.assertEquals(((StructTypeInfo) typeInfo).getFieldTypeInfos().get(0).getTypeName(),
                        "CHAR(8)");
    Assert.assertEquals(((CharTypeInfo)((StructTypeInfo) typeInfo).getFieldTypeInfos().get(0)).getLength(),
                        8);
    validateStruct(((StructTypeInfo) typeInfo).getFieldTypeInfos().get(1));

  }


  public void validateStruct(TypeInfo typeInfo) {
    Assert.assertTrue(typeInfo instanceof StructTypeInfo);
    Assert.assertEquals(typeInfo.getOdpsType(), OdpsType.STRUCT);

    StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
    Assert.assertEquals(structTypeInfo.getFieldNames().size(), 6);
    Assert.assertArrayEquals(structTypeInfo.getFieldNames().toArray(),
                             new String[]{"name", "age", "families", "hobbies", "test", "test2"});
    Assert.assertEquals(structTypeInfo.getFieldTypeInfos().get(0), TypeInfoFactory.STRING);
    Assert.assertEquals(structTypeInfo.getFieldTypeInfos().get(1), TypeInfoFactory.TINYINT);
    Assert.assertEquals(structTypeInfo.getFieldTypeInfos().get(2).getTypeName(),
                        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.INT)
                            .getTypeName());
    Assert.assertEquals(structTypeInfo.getFieldTypeInfos().get(3).getTypeName(),
                        TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING).getTypeName());
    Assert.assertEquals(structTypeInfo.getFieldTypeInfos().get(4).getTypeName(),
                        TypeInfoFactory.getDecimalTypeInfo(10, 2).getTypeName());
    Assert.assertEquals(structTypeInfo.getFieldTypeInfos().get(5), TypeInfoFactory.DECIMAL);
  }

  @Test
  public void testNegative() {
    String[]
        negatives =
        {"decim", "decimal(1,", "decimal(12)", "char1", "char(1", "varchar()", "varchar(a)",
         "struct<name:int,>", "struct<name:int", "struct<name,age:int>",
         "struct(name:int,age:int)", "struct<name:int;age:int>, map<int.string>", "array<int,>",
         "map<int, stri ng>"};

    int count = 0;

    for (String name : negatives) {
      try {
        TypeInfoParser.getTypeInfoFromTypeString(name);
      } catch (IllegalArgumentException e) {
        count++;
      }
    }

    Assert.assertEquals(count, negatives.length);
  }


  @Test
  public void testSpace() {
    String name = "map< string,   "
                  + "\t\r\n int>";

    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof MapTypeInfo);
    Assert.assertEquals(TypeInfoFactory.STRING, ((MapTypeInfo)typeInfo).getKeyTypeInfo());
    Assert.assertEquals(TypeInfoFactory.INT, ((MapTypeInfo)typeInfo).getValueTypeInfo());
    Assert.assertEquals(typeInfo.getTypeName(), "MAP<STRING,INT>");

    name = "struct< a : int , un  :     char(10)>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof  StructTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), "STRUCT<`a`:INT,`un`:CHAR(10)>");

    name = "struct< a b : int, un xs:     char(20)>";
    typeInfo = TypeInfoParser.getTypeInfoFromTypeString(name);
    Assert.assertTrue(typeInfo instanceof StructTypeInfo);
    Assert.assertEquals(typeInfo.getTypeName(), "STRUCT<`a b`:INT,`un xs`:CHAR(20)>");
    Assert.assertEquals(((StructTypeInfo)typeInfo).getFieldNames().get(0), "a b");
  }
}
