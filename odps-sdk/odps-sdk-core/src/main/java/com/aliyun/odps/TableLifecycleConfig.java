package com.aliyun.odps;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenyu
 * @date 2024/1/30 19:04
 */

public class TableLifecycleConfig {

    public enum TableLifecycleConfigItemEnum {
        /**
         * condition types to set lifecycle for tiered storage table
         * */
        DaysAfterLastModificationGreaterThan("DaysAfterLastModificationGreaterThan"),
        DaysAfterLastAccessGreaterThan("DaysAfterLastAccessGreaterThan"),
        DaysAfterLastTierModificationGreaterThan("DaysAfterLastTierModificationGreaterThan");

        private final String name;

        TableLifecycleConfigItemEnum(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static TableLifecycleConfigItemEnum getByName(String name) {
            for (TableLifecycleConfigItemEnum item : TableLifecycleConfigItemEnum.values()) {
                if (item.getName().equals(name)) {
                    return item;
                }
            }
            return null;
        }
    }

    /**
     * conditions to change storage type to low frequency storage
     */
    private Map<String, String> tierToLowFrequency;

    /**
     * conditions to change storage type to longterm storage
     */
     private Map<String, String> tierToLongterm;

    public Map<String, String> getTierToLowFrequency() {
        return tierToLowFrequency;
    }

    public Map<String, String> getTierToLongTerm() {
        return tierToLongterm;
    }

    /**
     * parse TableLifecycleConfigItem instance by json string
     *
     * @param jsonStr json string
     * */
    public static TableLifecycleConfig parse(String jsonStr){
        JsonObject jsonObject = new JsonParser().parse(jsonStr).getAsJsonObject();
        return parse(jsonObject);
    }

    public static TableLifecycleConfig parse(JsonObject jsonObject) {
        if (jsonObject == null || jsonObject.has("LifecycleConfig") == false) {
            return null;
        }
        TableLifecycleConfig tableLifecycleConfig = new TableLifecycleConfig();
        String lifecycleConfigJson = jsonObject.get("LifecycleConfig").getAsString();
        JsonObject lifecycleConfig = new JsonParser().parse(lifecycleConfigJson).getAsJsonObject();
        //
        if (lifecycleConfig.has("TierToLowFrequency")) {
            JsonObject tierToLowFrequency = lifecycleConfig.getAsJsonObject("TierToLowFrequency");
            tableLifecycleConfig.tierToLowFrequency = parseTableLifecycleConfigItem(tierToLowFrequency);
        }
        if (lifecycleConfig.has("TierToLongterm")) {
            JsonObject tierToLongTerm = lifecycleConfig.getAsJsonObject("TierToLongterm");
            tableLifecycleConfig.tierToLongterm = parseTableLifecycleConfigItem(tierToLongTerm);
        }
        //
        return tableLifecycleConfig;
    }

    private static Map<String, String> parseTableLifecycleConfigItem(JsonObject jsonObject) {
        Map<String, String> tableLifecycleConfigItem = new HashMap<>();
        if (jsonObject == null ) {
            return tableLifecycleConfigItem;
        }
        for (TableLifecycleConfigItemEnum item : TableLifecycleConfigItemEnum.values()) {
            if (jsonObject.has(item.getName())) {
                tableLifecycleConfigItem.put(item.getName(), jsonObject.get(item.getName()).getAsString());
            }
        }
        //
        return tableLifecycleConfigItem;
    }
}
