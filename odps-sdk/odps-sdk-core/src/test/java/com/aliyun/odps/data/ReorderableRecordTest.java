package com.aliyun.odps.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ReorderableRecordTest {

  @Test
  public void testSetStruct() {
    TypeInfo orderedStructType = getOrderedStructType();
    TableSchema schema = TableSchema.builder()
        .withStringColumn("c1")
        .withBigintColumn("c2")
        .withColumn(new Column("c3", orderedStructType)).build();
    Record record = new ReorderableRecord(schema);
    Struct misOrderedStruct = getMisOrderedStruct();
    System.out.println(misOrderedStruct);
    String
        expect =
        "{config:[{money:1234, age:25, name:Jason}, {money:1234, age:25, name:Jason}, {money:1234, age:25, name:Jason}], metadata:{key1=[{money:1234, age:25, name:Jason}, {money:1234, age:25, name:Jason}, {money:1234, age:25, name:Jason}], key2=[{money:1234, age:25, name:Jason}, {money:1234, age:25, name:Jason}, {money:1234, age:25, name:Jason}], key3=[{money:1234, age:25, name:Jason}, {money:1234, age:25, name:Jason}, {money:1234, age:25, name:Jason}]}}";
    Assert.assertEquals(expect, misOrderedStruct.toString());
    record.set("C3", misOrderedStruct);
    record.set("C2", 123L);
    record.set("C1", "test");
    System.out.println(record.get("C3"));
    expect =
        "{metadata:{key1=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key2=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key3=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, config:[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}";
    Assert.assertEquals(expect, record.get("C3").toString());
  }

  @Test
  public void testSetArray() {
    TypeInfo structType = getOrderedStructType();
    ArrayTypeInfo arrayTypeInfo = TypeInfoFactory.getArrayTypeInfo(structType);
    TableSchema schema = TableSchema.builder()
        .withStringColumn("c1")
        .withBigintColumn("c2")
        .withColumn(new Column("c3", arrayTypeInfo)).build();
    Record record = new ReorderableRecord(schema);

    Struct misOrderedStruct = getMisOrderedStruct();
    List<Struct> misOrderedArray = new ArrayList<>();
    misOrderedArray.add(misOrderedStruct);
    misOrderedArray.add(misOrderedStruct);
    misOrderedArray.add(misOrderedStruct);

    record.set("C3", misOrderedArray);
    System.out.println(record.get("C3"));
    String
        expect =
        "[{metadata:{key1=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key2=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key3=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, config:[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, {metadata:{key1=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key2=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key3=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, config:[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, {metadata:{key1=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key2=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key3=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, config:[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}]";
    Assert.assertEquals(expect, record.get("C3").toString());
  }

  @Test
  public void testSetMap() {
    TypeInfo structType = getOrderedStructType();
    TypeInfo mapTypeInfo = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, structType);
    TableSchema schema = TableSchema.builder()
        .withStringColumn("c1")
        .withBigintColumn("c2")
        .withColumn(new Column("c3", mapTypeInfo)).build();
    Record record = new ReorderableRecord(schema);

    Struct misOrderedStruct = getMisOrderedStruct();
    Map<String, Struct> misOrderedArray = new HashMap<>();
    misOrderedArray.put("key1", misOrderedStruct);
    misOrderedArray.put("key2", misOrderedStruct);
    misOrderedArray.put("key3", misOrderedStruct);

    record.set("C3", misOrderedArray);
    System.out.println(record.get("C3"));
    String
        expect =
        "{key1={metadata:{key1=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key2=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key3=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, config:[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, key2={metadata:{key1=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key2=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key3=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, config:[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, key3={metadata:{key1=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key2=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}], key3=[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}, config:[{name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}, {name:Jason, age:25, money:1234}]}}";//
    Assert.assertEquals(expect, record.get("C3").toString());
  }


  private TypeInfo getOrderedStructType() {
    // 1. 创建最内层的 Struct 类型（例如：Person）
    List<TypeInfo> personFieldTypes = new ArrayList<>();
    List<String> personFieldNames = new ArrayList<>();

// Person 结构体字段：name (String), age (Int)
    personFieldTypes.add(TypeInfoFactory.STRING); // name
    personFieldTypes.add(TypeInfoFactory.INT);    // age
    personFieldTypes.add(TypeInfoFactory.BIGINT);    // age
    personFieldNames.add("name");
    personFieldNames.add("age");
    personFieldNames.add("money");

    StructTypeInfo
        personStructType =
        TypeInfoFactory.getStructTypeInfo(personFieldNames, personFieldTypes);

// 2. 创建 Array 类型（元素类型为 Person Struct）
    ArrayTypeInfo personArrayType = TypeInfoFactory.getArrayTypeInfo(personStructType);

// 3. 创建 Map 类型（键为 String，值为 Person Array）
    MapTypeInfo personMapType = TypeInfoFactory.getMapTypeInfo(
        TypeInfoFactory.STRING,  // key type: String
        personArrayType          // value type: Array<Person>
    );

// 4. 创建外层 Struct 类型（包含 Map 和 Array 的嵌套）
    List<TypeInfo> outerFieldTypes = new ArrayList<>();
    List<String> outerFieldNames = new ArrayList<>();

// 外层结构体字段：metadata (Map<String, Array<Person>>), config (Array<Person>)
    outerFieldTypes.add(personMapType);     // metadata
    outerFieldTypes.add(personArrayType);   // config
    outerFieldNames.add("metadata");
    outerFieldNames.add("config");

    StructTypeInfo
        outerStructType =
        TypeInfoFactory.getStructTypeInfo(outerFieldNames, outerFieldTypes);

    return outerStructType;
  }

  private Struct getMisOrderedStruct() {
    // 1. 创建最内层的 Struct 类型（例如：Person）
    List<TypeInfo> personFieldTypes = new ArrayList<>();
    List<String> personFieldNames = new ArrayList<>();

// Person 结构体字段：name (String), age (Int)
    personFieldTypes.add(TypeInfoFactory.BIGINT); // money
    personFieldTypes.add(TypeInfoFactory.INT);    // age
    personFieldTypes.add(TypeInfoFactory.STRING);    // name
    personFieldNames.add("money");
    personFieldNames.add("age");
    personFieldNames.add("name");

    StructTypeInfo
        personStructType =
        TypeInfoFactory.getStructTypeInfo(personFieldNames, personFieldTypes);

    List<Object> value = new ArrayList<>();
    value.add(1234L);
    value.add(25);
    value.add("Jason");
    Struct person = new SimpleStruct(personStructType, value);

// 2. 创建 Array 类型（元素类型为 Person Struct）
    ArrayTypeInfo personArrayType = TypeInfoFactory.getArrayTypeInfo(personStructType);

    List<Struct> personArray = new ArrayList<>();
    personArray.add(person);
    personArray.add(person);
    personArray.add(person);

// 3. 创建 Map 类型（键为 String，值为 Person Array）
    MapTypeInfo personMapType = TypeInfoFactory.getMapTypeInfo(
        TypeInfoFactory.STRING,  // key type: String
        personArrayType          // value type: Array<Person>
    );

    Map<String, List<Struct>> personMap = new HashMap<>();
    personMap.put("key1", personArray);
    personMap.put("key2", personArray);
    personMap.put("key3", personArray);

// 4. 创建外层 Struct 类型（包含 Map 和 Array 的嵌套）
    List<TypeInfo> outerFieldTypes = new ArrayList<>();
    List<String> outerFieldNames = new ArrayList<>();

// 外层结构体字段：config (Array<Person>) metadata (Map<String, Array<Person>>),
    outerFieldTypes.add(personArrayType);   // config
    outerFieldTypes.add(personMapType);     // metadata
    outerFieldNames.add("config");
    outerFieldNames.add("metadata");

    StructTypeInfo
        outerStructType =
        TypeInfoFactory.getStructTypeInfo(outerFieldNames, outerFieldTypes);

    List<Object> outerValues = new ArrayList<>();
    outerValues.add(personArray);
    outerValues.add(personMap);
    Struct outerStruct = new SimpleStruct(outerStructType, outerValues);
    return outerStruct;
  }
}
