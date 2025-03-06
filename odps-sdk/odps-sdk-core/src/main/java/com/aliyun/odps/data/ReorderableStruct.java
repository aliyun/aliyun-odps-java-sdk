package com.aliyun.odps.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.NestedTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ReorderableStruct extends SimpleStruct {

  /**
   * lower field name to index
   */
  private Map<String, Integer> fieldNameToIndex;

  /**
   * A implements of {@link Struct}, which can reorder values into mc table order
   *
   * @param type   type of the struct
   * @param values values of the struct
   *               be careful: the struct value list is a reference of this param
   */
  public ReorderableStruct(StructTypeInfo type, List<Object> values) {
    super(type, values);
    rebuildFieldNameIndexMap();
  }

  private void rebuildFieldNameIndexMap() {
    fieldNameToIndex = new HashMap<>();
    List<String> fieldNames = typeInfo.getFieldNames();
    for (int index = 0; index < typeInfo.getFieldCount(); index++) {
      fieldNameToIndex.put(fieldNames.get(index).toLowerCase(), index);
    }
  }

  public void set(String fieldName, Object value) {
    values.set(fieldNameToIndex.get(fieldName.toLowerCase()), value);
  }

  public Object get(String fieldName) {
    return getFieldValue(fieldNameToIndex.get(fieldName.toLowerCase()));
  }

  public synchronized void reorder(StructTypeInfo orderedType) {
    List<String> orderedTypeFieldNames = orderedType.getFieldNames();
    if (orderedTypeFieldNames.size() != typeInfo.getFieldNames().size()) {
      throw new IllegalArgumentException("The orderedType is not compatible with this struct");
    }

    List<Object> newValues = new ArrayList<>();
    List<TypeInfo> newTypeInfos = orderedType.getFieldTypeInfos();

    for (int index = 0; index < orderedType.getFieldCount(); index++) {
      String colName = orderedTypeFieldNames.get(index).toLowerCase();
      Integer oldIndex = fieldNameToIndex.get(colName);
      if (oldIndex == null) {
        throw new IllegalArgumentException("Field " + colName + " not found in original struct");
      }
      Object oldValue = values.get(oldIndex);
      TypeInfo oldTypeInfo = typeInfo.getFieldTypeInfos().get(oldIndex);
      TypeInfo newTypeInfo = newTypeInfos.get(index);

      // Handle nested types
      if (oldTypeInfo instanceof NestedTypeInfo) {
        Object reorderedValue = reorderNestedType(oldValue, oldTypeInfo, newTypeInfo);
        newValues.add(reorderedValue);
      } else {
        newValues.add(oldValue);
      }
    }
    // update internal state
    this.values = newValues;
    this.typeInfo = orderedType;
    rebuildFieldNameIndexMap();
  }

  public static Object reorderNestedType(Object value, TypeInfo oldTypeInfo, TypeInfo newTypeInfo) {
    if (oldTypeInfo != null && oldTypeInfo.getOdpsType() != newTypeInfo.getOdpsType()) {
      throw new IllegalArgumentException("The nested type is not compatible");
    }
    if (newTypeInfo instanceof StructTypeInfo) {
      if (oldTypeInfo == null) {
        oldTypeInfo = ((Struct) value).getTypeInfo();
      }
      // Automatically wrap ordinary Struct as ReorderableStruct
      if (!(value instanceof ReorderableStruct)) {
        value = new ReorderableStruct((StructTypeInfo) oldTypeInfo, ((Struct) value).getFieldValues());
      }
      // Recursively process nested structures
      ReorderableStruct struct = (ReorderableStruct) value;
      struct.reorder((StructTypeInfo) newTypeInfo);
      return struct;
    } else if (newTypeInfo instanceof ArrayTypeInfo) {
      ArrayTypeInfo newArrayTypeInfo = (ArrayTypeInfo) newTypeInfo;
      TypeInfo newElementTypeInfo = newArrayTypeInfo.getElementTypeInfo();

      if (newElementTypeInfo instanceof NestedTypeInfo) {
        // Recursively process nested structures
        List<?> originalList = (List<?>) value;
        List<Object> newList = new ArrayList<>();
        for (Object element : originalList) {
          newList.add(reorderNestedType(element, null, newElementTypeInfo));
        }
        return newList;
      }
    } else if (newTypeInfo instanceof MapTypeInfo) {
      MapTypeInfo newMapTypeInfo = (MapTypeInfo) newTypeInfo;
      TypeInfo newKeyTypeInfo = newMapTypeInfo.getKeyTypeInfo();
      TypeInfo newValueTypeInfo = newMapTypeInfo.getValueTypeInfo();

      if (newKeyTypeInfo instanceof NestedTypeInfo
          || newValueTypeInfo instanceof NestedTypeInfo) {
        // Recursively process nested structures
        Map<?, ?> originalMap = (Map<?, ?>) value;
        Map<Object, Object> newMap = new HashMap<>();
        for (Map.Entry<?, ?> entry : originalMap.entrySet()) {
          Object reorderedKey = entry.getKey();
          if (newKeyTypeInfo instanceof NestedTypeInfo) {
            reorderedKey =
                reorderNestedType(entry.getKey(), null, newKeyTypeInfo);
          }
          Object reorderedValue = entry.getValue();

          if (newValueTypeInfo instanceof NestedTypeInfo) {
            reorderedValue =
                reorderNestedType(entry.getValue(), null, newValueTypeInfo);
          }
          newMap.put(reorderedKey, reorderedValue);
        }
        return newMap;
      }
    }
    // The basic type or type that does not require reordering will return directly to the original value
    return value;
  }
}
