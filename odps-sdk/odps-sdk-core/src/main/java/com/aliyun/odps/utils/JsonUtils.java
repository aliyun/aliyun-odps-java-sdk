package com.aliyun.odps.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class JsonUtils {

    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    /**
     * Serialize objects as JSON strings
     *
     * @param object to serialize
     * @return JSON strings
     */
    public static String toJson(Object object) {
        return gson.toJson(object);
    }

    /**
     * Serialize objects as formatted JSON strings (with indentation)
     *
     * @param object to serialize
     * @return Formatted JSON strings
     */
    public static String toPrettyJson(Object object) {
        return new GsonBuilder().setPrettyPrinting().create().toJson(object);
    }

    /**
     * Deserializes JSON strings to objects of the specified type
     *
     * @param json JSON strings
     * @param clazz target object type
     * @param <T> Generic type
     * @return Deserialized object
     */
    public static <T> T fromJson(String json, Class<T> clazz) {
        return gson.fromJson(json, clazz);
    }

    /**
     * Deserialize JSON strings to the specified generic type
     *
     * @param json JSON strings
     * @param type target generic type (obtained via TypeToken)
     * @param <T> Generic type
     * @return Deserialized object
     */
    public static <T> T fromJson(String json, Type type) {
        return gson.fromJson(json, type);
    }

    /**
     * Safely deserialize JSON strings as objects, and return Optional.empty() on failure
     *
     * @param json JSON strings
     * @param clazz target object type
     * @param <T> Generic type
     * @return Contains the Optional of the deserialized object
     */
    public static <T> Optional<T> safeFromJson(String json, Class<T> clazz) {
        try {
            return Optional.of(gson.fromJson(json, clazz));
        } catch (JsonSyntaxException | NullPointerException e) {
            return Optional.empty();
        }
    }

    /**
     * Convert JSON strings to Map<String, Object>
     *
     * @param json JSON strings
     * @return Map<String, Object>
     */
    public static Map<String, Object> toMap(String json) {
        return fromJson(json, new TypeToken<HashMap<String, Object>>() {}.getType());
    }

    /**
     * Convert JSON strings to <T>Lists
     *
     * @param json JSON strings
     * @param type element
     * @param <T> Generic type
     * @return List<T>
     */
    public static <T> List<T> toList(String json, Type type) {
        return fromJson(json, new TypeToken<List<T>>() {}.getType());
    }

    /**
     * Convert JSON strings to JsonObject (for scenarios that require manual field parsing)
     *
     * @param json JSON strings
     * @return JsonObject
     */
    public static JsonObject parseJsonObject(String json) {
        return gson.fromJson(json, JsonObject.class);
    }

    /**
     * Convert JSON strings to JsonElement (for dynamic parsing)
     *
     * @param json JSON strings
     * @return JsonElement
     */
    public static JsonElement parseJsonElement(String json) {
        return gson.fromJson(json, JsonElement.class);
    }

    /**
     * Get a default Gson instance (can be used for custom configurations)
     *
     * @return Gson instance
     */
    public static Gson getGson() {
        return gson;
    }
}