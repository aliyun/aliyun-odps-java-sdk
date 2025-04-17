package com.aliyun.odps.task;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.OdpsTestUtils;
import com.aliyun.odps.options.CreateInstanceOption;
import com.aliyun.odps.options.SQLTaskOption;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class UniqueIdentifyIDTest {

  @Test
  public void testCreateInstance() throws OdpsException {
    Odps odps = OdpsTestUtils.newDefaultOdps();
    odps.setDefaultProject("meta");

    CreateInstanceOption createInstanceOption = new CreateInstanceOption.Builder()
        .setUniqueIdentifyID("123456")
        .build();
    SQLTaskOption sqlTaskOption = new SQLTaskOption.Builder()
        .setInstanceOption(createInstanceOption)
        .build();
    Instance instance = SQLTask.run(odps, "select 1;", sqlTaskOption);
    String instanceId = instance.getId();
    System.out.println(instance.getId());

    Instance instance2 = SQLTask.run(odps, "select 2;", sqlTaskOption);
    System.out.println(instance2.getId());
    Assert.assertEquals(instanceId, instance2.getId());
  }
}
