package com.aliyun.odps.task;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.OdpsTestUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MergeTaskTest {

  @Test
  @Ignore
  public void testMergeTask() throws Exception {
    run("create table if not exists mf_dt (pk bigint not null primary key, val bigint not null) \n"
        + "             partitioned by (dd string, hh string) \n"
        + "             tblproperties (\"transactional\"=\"true\");");
    run("insert into table mf_dt partition(dd='01', hh='01') values (1, 1), (2, 2);");
    run("insert into table mf_dt partition(dd='01', hh='01') values (2, 20), (3, 3);");
    run("insert into table mf_dt partition(dd='01', hh='01') values (3, 30), (4, 4);");

    Map<String, String> hints = new HashMap<>();
    hints.put("odps.merge.task.mode", "service");
    run("alter table mf_dt partition(dd='01', hh='01') compact major;");
  }

  private void run(String sql) throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    Instance instance = SQLTask.run(odps, sql);
    System.out.println(sql);
    System.out.println(odps.logview().generateLogView(instance, 24));
    instance.waitForSuccess();

  }
}
