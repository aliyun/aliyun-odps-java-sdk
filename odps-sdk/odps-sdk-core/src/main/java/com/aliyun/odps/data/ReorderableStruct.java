package com.aliyun.odps.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.aliyun.odps.exceptions.SchemaMismatchRuntimeException;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.NestedTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.StringUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ReorderableStruct implements Struct {

  protected StructTypeInfo typeInfo;
  protected List<Object> values;

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
    if (type == null || values == null) {
      throw new IllegalArgumentException("Illegal arguments for StructObject.");
    }
    if (values.size() != type.getFieldCount()) {
      throw new SchemaMismatchRuntimeException(
          "Schema mismatch, expect " + type.getFieldCount() + " columns, but got "
          + values.size());
    }
    this.typeInfo = type;
    this.values = values;
    rebuildFieldNameIndexMap();
  }

  public ReorderableStruct(StructTypeInfo type) {
    if (type == null) {
      throw new IllegalArgumentException("Illegal arguments for StructObject.");
    }
    this.typeInfo = type;
    values = new ArrayList<>(type.getFieldCount());
    for (int i = 0; i < type.getFieldCount(); i++) {
      values.add(null);
    }
    rebuildFieldNameIndexMap();
  }

  private void rebuildFieldNameIndexMap() {
    fieldNameToIndex = new HashMap<>();
    List<String> fieldNames = typeInfo.getFieldNames();
    for (int index = 0; index < typeInfo.getFieldCount(); index++) {
      fieldNameToIndex.put(fieldNames.get(index).toLowerCase(), index);
    }
  }

  public void setFieldValue(String fieldName, Object value) {
    values.set(fieldNameToIndex.get(fieldName.toLowerCase()), value);
  }

  public void setFieldValue(int index, Object value) {
    values.set(index, value);
  }

  @Override
  public Object getFieldValue(String fieldName) {
    return getFieldValue(fieldNameToIndex.get(fieldName.toLowerCase()));
  }

  @Override
  public Object getFieldValue(int index) {
    return values.get(index);
  }

  @Override
  public TypeInfo getFieldTypeInfo(String fieldName) {
    return typeInfo.getFieldTypeInfos().get(fieldNameToIndex.get(fieldName));
  }

  public synchronized void reorder(StructTypeInfo orderedType) {
    List<String> orderedTypeFieldNames = orderedType.getFieldNames();
    if (orderedTypeFieldNames.size() != typeInfo.getFieldNames().size()) {
      throw new SchemaMismatchRuntimeException(
          "The orderedType is not compatible with this struct. Struct has "
          + typeInfo.getFieldNames().size() + " columns, but record schema need "
          + orderedTypeFieldNames.size() + " columns.");
    }

    List<Object> newValues = new ArrayList<>();
    List<TypeInfo> newTypeInfos = orderedType.getFieldTypeInfos();

    for (int index = 0; index < orderedType.getFieldCount(); index++) {
      String colName = orderedTypeFieldNames.get(index).toLowerCase();
      Integer oldIndex = fieldNameToIndex.get(colName);
      if (oldIndex == null) {
        throw new SchemaMismatchRuntimeException(
            "Field " + colName + " not found in original struct");
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

  public static Object reorderNestedType(Object value, TypeInfo oldTypeInfo,
                                         TypeInfo newTypeInfo) {
    if (value == null) {
      return null;
    }
    if (oldTypeInfo != null && oldTypeInfo.getOdpsType() != newTypeInfo.getOdpsType()) {
      throw new SchemaMismatchRuntimeException(
          "The nested type is not compatible. " + oldTypeInfo.getTypeName() + " != "
          + newTypeInfo.getTypeName());
    }
    if (newTypeInfo instanceof StructTypeInfo) {
      if (oldTypeInfo == null) {
        oldTypeInfo = ((Struct) value).getTypeInfo();
      }
      // Automatically wrap ordinary Struct as ReorderableStruct
      if (value instanceof Struct && !(value instanceof ReorderableStruct)) {
        value =
            new ReorderableStruct((StructTypeInfo) oldTypeInfo,
                                  ((Struct) value).getFieldValues());
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

  @Override
  public int getFieldCount() {
    return values.size();
  }

  @Override
  public String getFieldName(int index) {
    return typeInfo.getFieldNames().get(index);
  }

  @Override
  public TypeInfo getFieldTypeInfo(int index) {
    return typeInfo.getFieldTypeInfos().get(index);
  }

  @Override
  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  @Override
  public List<Object> getFieldValues() {
    return values;
  }

  @Override
  public String toString() {
    String valueStr = "{";
    int colCount = getFieldCount();
    for (int i = 0; i < colCount; ++i) {
      valueStr += getFieldName(i) + ":" + getFieldValue(i);
      if (i != colCount - 1) {
        valueStr += ", ";
      }
    }
    valueStr += "}";
    return valueStr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ReorderableStruct that = (ReorderableStruct) o;

    if (!StringUtils.equalsIgnoreCase(this.typeInfo.getFieldNames(),
                                      that.typeInfo.getFieldNames())) {
      return false;
    }

    return Objects.equals(this.getFieldValues(), that.getFieldValues());
  }

  @Override
  public int hashCode() {
    return Objects.hash(StringUtils.toLowerCase(typeInfo.getFieldNames()), values);
  }
}
