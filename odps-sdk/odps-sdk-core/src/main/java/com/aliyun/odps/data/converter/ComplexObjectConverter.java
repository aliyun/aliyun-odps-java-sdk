package com.aliyun.odps.data.converter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

// json type is not supported in nested column type now
enum ComplexObjectConverter implements OdpsObjectConverter {

    JSON(ConverterConstant.COMPLEX_TYPE_FORMAT_JSON),
    JSON_ALL_STR(ConverterConstant.COMPLEX_TYPE_FORMAT_JSON_STR),
    HUMAN_READABLE(ConverterConstant.COMPLEX_TYPE_OUTPUT_FORMAT_HUMAN_READABLE);

    private final String type;
    private final boolean convertToString;

    ComplexObjectConverter(String type) {
        this.type = type;
        convertToString = ConverterConstant.COMPLEX_TYPE_FORMAT_JSON_STR.equalsIgnoreCase(type);
    }

    @Override
    public String format(Object object, TypeInfo typeInfo, OdpsRecordConverter converter) {
        switch (type) {
            case ConverterConstant.COMPLEX_TYPE_OUTPUT_FORMAT_HUMAN_READABLE: {
                return humanReadableFormat(object, typeInfo, converter);
            }
            case ConverterConstant.COMPLEX_TYPE_FORMAT_JSON:
            case ConverterConstant.COMPLEX_TYPE_FORMAT_JSON_STR: {
                return convertToJson(object, typeInfo, converter).toString();
            }
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public Object parse(String str, TypeInfo typeInfo, OdpsRecordConverter converter) {
        // check type
        switch (typeInfo.getOdpsType()) {
            case ARRAY:
            case MAP:
            case STRUCT:
                break;
            default:
                throw new IllegalArgumentException();
        }
        return parse(JsonParser.parseString(str), typeInfo, converter);
    }

    // parse jsonElement => java object
    public Object parse(JsonElement jsonElement, TypeInfo typeInfo, OdpsRecordConverter formatter) {
        if (jsonElement.isJsonNull()) {
            return null;
        }

        switch (typeInfo.getOdpsType()) {
            case ARRAY:
                if (!(formatter.objectConverterMap.get(OdpsType.ARRAY) instanceof ComplexObjectConverter)) {
                    // user customized converter
                    return formatter.parseObject(jsonElement.getAsString(), typeInfo);
                }

                List result = new ArrayList();

                JsonArray jsonArray = jsonElement.getAsJsonArray();
                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
                TypeInfo elementTypeInfo = arrayTypeInfo.getElementTypeInfo();

                for (JsonElement e: jsonArray) {
                    result.add(parse(e, elementTypeInfo, formatter));
                }
                return result;
            case MAP:
                if (!(formatter.objectConverterMap.get(OdpsType.MAP) instanceof ComplexObjectConverter)) {
                    // user customized converter
                    return formatter.parseObject(jsonElement.getAsString(), typeInfo);
                }

                Map resultMap = new HashMap();

                JsonObject jsonObject = jsonElement.getAsJsonObject();
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                TypeInfo keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
                TypeInfo valueTypeInfo = mapTypeInfo.getValueTypeInfo();

                for (Map.Entry<String, JsonElement> entry: jsonObject.entrySet()) {
                    Object key = formatter.parseObject(entry.getKey(), keyTypeInfo);
                    Object value = parse(entry.getValue(), valueTypeInfo, formatter);
                    resultMap.put(key, value);
                }

                return resultMap;
            case STRUCT:
                if (!(formatter.objectConverterMap.get(OdpsType.STRUCT) instanceof ComplexObjectConverter)) {
                    // user customized converter
                    return formatter.parseObject(jsonElement.getAsString(), typeInfo);
                }

                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<Object> values = new ArrayList<>();

                JsonObject struct = jsonElement.getAsJsonObject();

                for (int i = 0; i < structTypeInfo.getFieldCount(); i++) {
                    String name = structTypeInfo.getFieldNames().get(i);
                    TypeInfo valueType = structTypeInfo.getFieldTypeInfos().get(i);

                    Object value = parse(struct.get(name), valueType, formatter);
                    values.add(value);
                }

                return new SimpleStruct(structTypeInfo, values);
            case BOOLEAN:
                if (!convertToString) {
                    return jsonElement.getAsBoolean();
                }
            case TINYINT:
                if (!convertToString) {
                    return jsonElement.getAsByte();
                }
            case SMALLINT:
                if (!convertToString) {
                    return jsonElement.getAsShort();
                }
            case INT:
                if (!convertToString) {
                    return jsonElement.getAsInt();
                }
            case BIGINT:
                if (!convertToString) {
                    return jsonElement.getAsLong();
                }
            case FLOAT:
                if (!convertToString) {
                    return jsonElement.getAsFloat();
                }
            case DOUBLE:
                if (!convertToString) {
                    return jsonElement.getAsDouble();
                }
            case DECIMAL:
                if (!convertToString) {
                    return jsonElement.getAsBigDecimal();
                }
            default:
                return formatter.parseObject(jsonElement.getAsString(), typeInfo);
        }
    }

    private JsonElement convertToJson(Object object, TypeInfo typeInfo, OdpsRecordConverter formatter) {

        if (object == null) {
            return JsonNull.INSTANCE;
        }


        switch (typeInfo.getOdpsType()) {
            case ARRAY:
                if (formatter.objectConverterMap.get(OdpsType.ARRAY) != ComplexObjectConverter.JSON &&
                    formatter.objectConverterMap.get(OdpsType.ARRAY) != ComplexObjectConverter.JSON_ALL_STR) {
                    // user customized converter
                    return new JsonPrimitive(formatter.formatObject(object, typeInfo));
                }

                JsonArray jsonArray = new JsonArray();

                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
                TypeInfo elementType = arrayTypeInfo.getElementTypeInfo();

                List list = (List) object;
                for (Object listItem : list) {
                    JsonElement element = convertToJson(listItem, elementType, formatter);
                    jsonArray.add((element));
                }
                return jsonArray;
            case MAP:
                if (formatter.objectConverterMap.get(OdpsType.MAP) != ComplexObjectConverter.JSON &&
                    formatter.objectConverterMap.get(OdpsType.MAP) != ComplexObjectConverter.JSON_ALL_STR) {
                    // user customized converter
                    return new JsonPrimitive(formatter.formatObject(object, typeInfo));
                }

                JsonObject jsonObject = new JsonObject();

                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                TypeInfo keyInfo = mapTypeInfo.getKeyTypeInfo();
                TypeInfo valueInfo = mapTypeInfo.getValueTypeInfo();

                Map map = (Map) object;
                for (Object entryObject : map.entrySet()) {
                    Map.Entry entry = (Map.Entry) entryObject;
                    String keyString = formatter.formatObject(entry.getKey(), keyInfo);
                    JsonElement valString = convertToJson(entry.getValue(), valueInfo, formatter);

                    jsonObject.add(keyString, valString);
                }
                return jsonObject;
            case STRUCT:
                if (formatter.objectConverterMap.get(OdpsType.STRUCT) != ComplexObjectConverter.JSON &&
                    formatter.objectConverterMap.get(OdpsType.STRUCT) != ComplexObjectConverter.JSON_ALL_STR) {
                    // user customized converter
                    return new JsonPrimitive(formatter.formatObject(object, typeInfo));
                }

                JsonObject jsonStruct = new JsonObject();

                SimpleStruct struct = (SimpleStruct) object;
                for (int i = 0; i < struct.getFieldCount(); i++) {
                    TypeInfo fieldType = struct.getFieldTypeInfo(i);
                    String key = struct.getFieldName(i);
                    Object value = struct.getFieldValue(i);

                    jsonStruct.add(key, convertToJson(value, fieldType, formatter));
                }
                return jsonStruct;
            case BOOLEAN:
                if (convertToString) {
                    return new JsonPrimitive(formatter.formatObject(object, typeInfo));
                }
                return new JsonPrimitive((Boolean) object);
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                if (convertToString) {
                    // GSON allow NAN INFINITY
                    return new JsonPrimitive(formatter.formatObject(object, typeInfo));
                }
                return new JsonPrimitive((Number) object);

            case DECIMAL:
                if (convertToString) {
                    // GSON allow NAN INFINITY
                    return new JsonPrimitive(formatter.formatObject(object, typeInfo));
                }
                Number n = new Number() {
                    @Override
                    public int intValue() {
                        return 0;
                    }

                    @Override
                    public long longValue() {
                        return 0;
                    }

                    @Override
                    public float floatValue() {
                        return 0;
                    }

                    @Override
                    public double doubleValue() {
                        return 0;
                    }

                    @Override
                    public String toString() {
                        // 3.0 => 3, same as sql
                        return ((BigDecimal)object).stripTrailingZeros().toPlainString();
                    }
                };
                return new JsonPrimitive(n);
            case DATE:
            case DATETIME:
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
            case BINARY:
            case CHAR:
            case VARCHAR:
            case STRING:
                return new JsonPrimitive(formatter.formatObject(object, typeInfo));
            default:
                throw new IllegalArgumentException("unsupported odpstype: " + typeInfo);
        }

    }

    private String humanReadableFormat(Object object, TypeInfo typeInfo, OdpsRecordConverter formatter) {
        if (object == null) {
            return "null";
        }

        StringBuilder sb = new StringBuilder();
        switch (typeInfo.getOdpsType()) {
            case ARRAY:
                if (formatter.objectConverterMap.get(OdpsType.ARRAY) != ComplexObjectConverter.HUMAN_READABLE) {
                    // user customized converter
                    return formatter.formatObject(object, typeInfo);
                }

                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
                List list = (List) object;

                sb.append("[");

                for (int i = 0; i < list.size(); i++) {
                    Object element = list.get(i);
                    sb.append(formatter.formatObject(element, arrayTypeInfo.getElementTypeInfo()));
                    if (i < list.size() - 1) {
                        sb.append(", ");
                    }
                }
                return sb.append("]").toString();
            case MAP:
                if (formatter.objectConverterMap.get(OdpsType.MAP) != ComplexObjectConverter.HUMAN_READABLE) {
                    // user customized converter
                    return formatter.formatObject(object, typeInfo);
                }

                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                Map map = (Map) object;

                sb.append("{");
                TypeInfo keyTypeInfo = mapTypeInfo.getKeyTypeInfo();
                TypeInfo valueTypeInfo = mapTypeInfo.getValueTypeInfo();


                int cnt = 0;
                int size = map.entrySet().size();
                for (Object entry: map.entrySet()) {
                    Map.Entry entry1 = (Map.Entry) entry;
                    Object key = entry1.getKey();
                    Object value = entry1.getValue();
                    sb.append(formatter.formatObject(key, keyTypeInfo));
                    sb.append(":");
                    sb.append(formatter.formatObject(value, valueTypeInfo));


                    if (cnt < size - 1) {
                        sb.append(", ");
                    }
                    cnt++;
                }
                return sb.append("}").toString();
            case STRUCT:
                if (formatter.objectConverterMap.get(OdpsType.STRUCT)!= ComplexObjectConverter.HUMAN_READABLE) {
                    // user customized converter
                    return formatter.formatObject(object, typeInfo);
                }

                Struct struct = (Struct) object;
                sb.append("{");

                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                for (int i = 0; i < structTypeInfo.getFieldCount(); i++) {
                    String name = struct.getFieldName(i);
                    TypeInfo typeInfo1 = struct.getFieldTypeInfo(i);
                    sb.append(name).append(":");
                    String valueFormat = formatter.formatObject(struct.getFieldValue(i), typeInfo1);
                    sb.append(valueFormat);

                    if (i < struct.getFieldCount() - 1) {
                        sb.append(", ");
                    }
                }

                return sb.append("}").toString();
            default:
                throw new IllegalArgumentException("not supported type");
        }
    }

}
