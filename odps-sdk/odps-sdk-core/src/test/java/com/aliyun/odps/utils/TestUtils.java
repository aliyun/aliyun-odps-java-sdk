package com.aliyun.odps.utils;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;

public class TestUtils {

    public static void executeSql(Odps odps, String sql) throws OdpsException {
        Instance i = SQLTask.run(odps, sql);
        i.waitForSuccess();
    }

    public static void executeSql(Odps odps, String sql, Map<String, String> hints) throws OdpsException {
        Instance i = SQLTask.run(odps, odps.getDefaultProject(), sql, hints, null);
        System.out.println(new LogView(odps).generateLogView(i, 24));
        i.waitForSuccess();
    }

    public static void executeSqlScript(Odps odps, String sql) throws OdpsException {
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.sql.submit.mode", "script");
        executeSql(odps, sql, hints);
    }

}
