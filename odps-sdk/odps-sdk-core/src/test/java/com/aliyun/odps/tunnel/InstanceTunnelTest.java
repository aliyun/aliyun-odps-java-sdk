package com.aliyun.odps.tunnel;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.GeneralConfiguration;

public class InstanceTunnelTest extends TestBase {

  @Test
  public void timeoutConfigTest() {

    InstanceTunnel instanceTunnel = new InstanceTunnel(odps);

    GeneralConfiguration confOld = instanceTunnel.getConfig();
    Assert.assertEquals(180, confOld.getSocketConnectTimeout());
    Assert.assertEquals(300, confOld.getSocketTimeout());

    confOld.setSocketTimeout(10);
    confOld.setSocketConnectTimeout(20);

    GeneralConfiguration confNew = instanceTunnel.getConfig();
    Assert.assertEquals(20, confNew.getSocketConnectTimeout());
    Assert.assertEquals(10, confNew.getSocketTimeout());

  }

}
