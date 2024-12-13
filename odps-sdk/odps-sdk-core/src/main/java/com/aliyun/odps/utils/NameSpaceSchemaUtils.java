package com.aliyun.odps.utils;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.commons.transport.Params;

public class NameSpaceSchemaUtils {

    public static String getFullName(String project, String schema, String objectName) {
        if (isSchemaEnabled(schema)) {
            return CommonUtils.quoteRef(project) + "." + CommonUtils.quoteRef(schema) + "."
                   + CommonUtils.quoteRef(objectName);
        } else {
            // two-tier                 => a.b
            // three-tier not reload    => a.null.b
            // three-tier reloaded      => a.default.b
            return CommonUtils.quoteRef(project) + "." + CommonUtils.quoteRef(objectName);
        }
    }

    public static Map<String, String> setSchemaFlagInHints(Map<String, String> hints, String schemaName) {
        if (isSchemaEnabled(schemaName)) {
            if (hints == null) {
                hints = new HashMap<>();
            }
            if (!hints.containsKey(OdpsConstants.ODPS_NAMESPACE_SCHEMA)) {
                hints.put(OdpsConstants.ODPS_NAMESPACE_SCHEMA, "true");
            }
            if (!hints.containsKey("odps.sql.allow.namespace.schema")) {
                // TODO: HACK for internal test, remove this later
                hints.put("odps.sql.allow.namespace.schema", "true");
            }
        }
        return hints;
    }

    public static HashMap<String, String> initParamsWithSchema(String schemaName) {
        HashMap<String, String> params = new HashMap<>();
        if (isSchemaEnabled(schemaName)) {
            params.put(Params.ODPS_SCHEMA_NAME, schemaName);
        }
        return params;
    }

    public static boolean isSchemaEnabled(String schemaName) {
        return !StringUtils.isNullOrEmpty(schemaName);
    }

}
