package com.aliyun.odps;

import org.junit.Test;

public class SessionConnectionPoolTest extends TestBase {

  @Test
  public void TestConnectionPool() throws OdpsException {
    String sessionName = "session_pool_" + System.currentTimeMillis();

    Session session = SessionTest.createSession(sessionName);
  }
}
