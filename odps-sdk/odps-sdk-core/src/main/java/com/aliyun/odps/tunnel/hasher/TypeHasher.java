package com.aliyun.odps.tunnel.hasher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.OdpsType;

/**
 * Created by wlz on 17/2/28.
 */
public class TypeHasher {
    /**
     * version for hash
     */
    private static final Map<String, HasherFactory> versionsMap = new LinkedHashMap<>();
    private static final String defaultVersion = "default";
    private static final String legacyVersion = "legacy";

    static {
        /**
         * the mapping of version and HasherImplementation
         */
        versionsMap.put(defaultVersion, new DefaultHashFactory());
        versionsMap.put(legacyVersion, new LegacyHashFactory());
    }

    public static String getDefaultVersion() {
        return defaultVersion;
    }

    public static String getLegacyVersion() {
        return legacyVersion;
    }

    public static String getVersion(String version) {
        if (version == null || version.isEmpty() || !versionsMap.containsKey(version)) {
            return getDefaultVersion();
        }
        return version;
    }

    /**
     * 计算 hash 值
     * @param type odps 类型
     * @param value 数据值
     * @return hash 值
     */
    public static int hash(OdpsType type, Object value) {
        OdpsHasher hasher = getHasher(type, null);

        return hasher.hash(hasher.normalizeType(value));
    }

    /**
     * 计算 hash 值
     *
     * @param type odps 类型
     * @param value 数据值
     * @param version 版本，包括 legacy 和 default
     * @return hash 值
     */
    public static int hash(OdpsType type, Object value, String version) {
        OdpsHasher hasher = getHasher(type, version);

        return hasher.hash(hasher.normalizeType(value));
    }

    static OdpsHasher getHasher(OdpsType type) {
        return getHasher(type, null);
    }
    static OdpsHasher getHasher(OdpsType type, String version) {

        if (version == null || version.isEmpty()) {
            version = defaultVersion;
        }

        HasherFactory factory = versionsMap.get(version);
        if (factory == null) {
            throw new RuntimeException("Not supported hash function version:" + version);
        }

        OdpsHasher hasher = factory.getHasher(type);
        if (hasher == null) {
            throw new RuntimeException("Not supported hash function type:" + type.name());
        }
        return hasher;
    }

    /**
     * Get the hash val for one row
     * @param: the hashvals for all field of one row
     * @return: the final combine hash val for the row
     */
    public static int CombineHashVal(int hashVals[]) {
        int combineHashVal = 0;
        for (int hashVal : hashVals) {
            combineHashVal += hashVal;
        }
        return (combineHashVal ^ (combineHashVal >> 8));
    }

    public static int CombineHashVal(List<Integer> hashVals) {
        int combineHashVal = 0;
        for (int hashVal : hashVals) {
            combineHashVal += hashVal;
        }
        return (combineHashVal ^ (combineHashVal >> 8));
    }
}
