package com.aliyun.odps.tunnel.hasher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    public static OdpsHasher getHasher(String typeName, String version) {

        if (version == null || version.isEmpty()) {
            version = defaultVersion;
        }

        HasherFactory factory = versionsMap.get(version);
        if (factory == null) {
            throw new RuntimeException("Not supported hash function version:" + version);
        }

        OdpsHasher hasher = factory.getHasher(typeName.toLowerCase());
        if (hasher == null) {
            throw new RuntimeException("Not supported hash function type:" + typeName);
        }
        return hasher;
    }

    public static OdpsHasher getHasher(String typeName) {
        OdpsHasher hasher = getHasher(typeName, null);
        assert hasher != null;
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

    public static void main(String[] args) {

        OdpsHasher hasher = TypeHasher.getHasher("bigint", "legacy");
        int[] hash = new int[1];
        hash[0] = hasher.hash(0L);
        int prehash = CombineHashVal(hash) % 3;
        System.out.println(prehash);
        for (long i=0; i<10L; i++) {
            hash[0] = hasher.hash(i);
            int curhash = CombineHashVal(hash) % 3;
            System.out.println(i + ": " + curhash);
//            if (curhash != prehash) {
//                System.out.println(i);
//                System.out.println(curhash);
//                break;
//            }
        }
    }
}
