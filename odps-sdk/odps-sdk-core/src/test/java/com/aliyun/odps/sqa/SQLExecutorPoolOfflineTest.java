/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.sqa;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.rest.ErrorMessage;
import com.aliyun.odps.rest.RestException;

import org.junit.Assert;
import org.junit.Test;

/**
 * SQLExecutorPool 的离线单元测试。
 *
 * executeMode=OFFLINE 时构造 SQLExecutor 既不会 attach session,也不会解析 tunnel endpoint,
 * 所以这些用例不依赖任何 MaxCompute 环境,在没有测试项目的机器上也能稳定运行,
 * 用来固化连接池的记账与生命周期语义。
 */
public class SQLExecutorPoolOfflineTest {

  // 不会被访问:OFFLINE 模式的 executor 构造过程不发任何请求
  private static final String UNUSED_ENDPOINT = "http://127.0.0.1:1/api";

  private static Odps offlineOdps() {
    Odps odps = new Odps(new AliyunAccount("ut_access_id", "ut_access_key"));
    odps.setEndpoint(UNUSED_ENDPOINT);
    odps.setDefaultProject("ut_project_not_exists");
    return odps;
  }

  private static SQLExecutorPool newPool(int initPoolSize, int maxPoolSize) throws OdpsException {
    return SQLExecutorPoolBuilder.builder()
        .odps(offlineOdps())
        .executeMode(ExecuteMode.OFFLINE)
        .useInstanceTunnel(false)
        .initPoolSize(initPoolSize)
        .maxPoolSize(maxPoolSize)
        .build();
  }

  /**
   * 归还的 executor 必须回到空闲队列并能被再次借出(同一个对象)。
   *
   * 回归缺陷 86734391:池以 isActive() 作为唯一复用判据,而 OFFLINE 模式的 executor 不持有
   * session,isActive() 恒为 false,于是每次归还都被丢弃、每次借出都新建 —— 连接池退化为对象工厂;
   * 在 MCQA 下一次瞬时的 session 状态抖动同样会让池静默缩水。
   */
  @Test
  public void testReleasedExecutorIsReused() throws OdpsException {
    SQLExecutorPool pool = newPool(2, 3);
    Assert.assertEquals(2, pool.getExecutorCount());
    Assert.assertEquals(2, pool.getActiveCount());

    SQLExecutor e1 = pool.getExecutor();
    SQLExecutor e2 = pool.getExecutor();
    Assert.assertEquals(0, pool.getActiveCount());
    Assert.assertEquals(2, pool.getBusyCount());
    Assert.assertEquals(2, pool.getExecutorCount());

    // 归还的连接必须回到空闲队列,并且下一次借出的就是它(先进先出)
    pool.releaseExecutor(e1);
    Assert.assertEquals("归还后应回到空闲队列", 1, pool.getActiveCount());
    Assert.assertEquals(1, pool.getBusyCount());
    Assert.assertEquals("归还不应让池缩小", 2, pool.getExecutorCount());
    Assert.assertSame(e1, pool.getExecutor());

    pool.releaseExecutor(e2);
    Assert.assertEquals(1, pool.getActiveCount());
    Assert.assertSame(e2, pool.getExecutor());
    Assert.assertEquals("全程复用,不应新建连接", 2, pool.getExecutorCount());
  }

  /**
   * attach 失败必须留下可查的原因。
   *
   * 默认策略是 alwaysFallback:attach 失败不抛异常,只是静默变成 OFFLINE 执行,对外表现为
   * isActive() 恒为 false、连接池每次归还都淘汰。86734391/86734393/86734392 排查时,日志里
   * 只有一条 INFO 且不含 project/session 上下文,池淘汰时更是完全没有线索。
   * endpoint 指向 127.0.0.1:1,attach 立刻失败,不会发出真实请求。
   */
  @Test
  public void testAttachFailureReasonIsRecorded() throws OdpsException {
    SQLExecutor executor = SQLExecutorBuilder.builder()
        .odps(offlineOdps())
        .executeMode(ExecuteMode.INTERACTIVE)
        .useInstanceTunnel(false)
        .attachTimeout(1L)
        .serviceName("public.default")
        .build();
    Assert.assertTrue(executor instanceof SQLExecutorImpl);
    SQLExecutorImpl impl = (SQLExecutorImpl) executor;
    Assert.assertFalse("attach 未成功时 isActive() 应为 false", impl.isActive());
    Assert.assertNotNull("attach 失败必须记录原因", impl.getSessionAttachError());
    Assert.assertTrue("保留的是完整异常,不是 message 字符串",
        impl.getSessionAttachError() instanceof OdpsException);
    String reason = impl.describeSessionAttachError();
    Assert.assertTrue("原因应包含 session 名: " + reason, reason.contains("public.default"));
    Assert.assertTrue("原因应包含 project: " + reason, reason.contains("ut_project_not_exists"));
    Assert.assertTrue("原因应带上异常本身: " + reason,
        reason.contains(impl.getSessionAttachError().toString()));
    impl.close();
  }

  /**
   * attach 失败降级、从未拿到 session 的 executor,归还时必须回到空闲队列而不是被淘汰。
   *
   * 它不持有 session,没有"泄漏一个活着的 MCQA"的风险,和 executeMode=OFFLINE 创建的
   * executor 是同一类;判据应当是"有没有 session",而不是"创建时请求了哪种模式"。
   * 现状(30178937 合并后):这类 executor 走 isActive()==false 分支被淘汰,池在 MCQA
   * 故障期每次归还都要在锁内重新 build() 一次(带网络的 Session.attach),真实回归
   * SQLExecutorTest#testExecutorPoolNormal 因此断在 SQLExecutorTest.java:314。
   * endpoint 指向 127.0.0.1:1,attach 立刻被拒,不发真实请求(池的 builder 不暴露 attachTimeout,这里也不依赖它)。
   */
  @Test
  public void testFallbackExecutorWithoutSessionIsReused() throws OdpsException {
    SQLExecutorPool pool = SQLExecutorPoolBuilder.builder()
        .odps(offlineOdps())
        .executeMode(ExecuteMode.INTERACTIVE)
        .useInstanceTunnel(false)
        .serviceName("public.default")
        .initPoolSize(1)
        .maxPoolSize(2)
        .build();
    Assert.assertEquals(1, pool.getExecutorCount());
    Assert.assertEquals(1, pool.getActiveCount());

    SQLExecutor executor = pool.getExecutor();
    Assert.assertTrue(executor instanceof SQLExecutorImpl);
    Assert.assertTrue("前置:attach 未成功", !((SQLExecutorImpl) executor).isActive());
    Assert.assertEquals(0, pool.getActiveCount());

    pool.releaseExecutor(executor);
    Assert.assertEquals("无 session 的降级 executor 应被复用,不是被淘汰", 1, pool.getActiveCount());
    Assert.assertEquals("复用不应让池缩小或重建", 1, pool.getExecutorCount());
    Assert.assertSame(executor, pool.getExecutor());

    pool.releaseExecutor(executor);
    pool.close();
  }

  /**
   * 服务端的 errorCode 与 RequestId 必须出现在摘要里——那是拿去服务端反查这次 attach 的唯一键。
   *
   * 形状按真实抛点构造:OdpsException(cause=RestException),错误体来自服务端 XML。
   * 不发任何请求,所以不依赖环境。
   */
  @Test
  public void testAttachFailureSummaryKeepsErrorCodeAndRequestId() throws Exception {
    SQLExecutor executor = SQLExecutorBuilder.builder()
        .odps(offlineOdps())
        .executeMode(ExecuteMode.OFFLINE)
        .useInstanceTunnel(false)
        .build();
    SQLExecutorImpl impl = (SQLExecutorImpl) executor;
    Assert.assertNull("OFFLINE 模式构造期不该有降级原因", impl.describeSessionAttachError());

    ErrorMessage error = ErrorMessage.from(
        ("<Error><Code>ODPS-0430015</Code><Message>Request table not exist</Message>"
            + "<RequestId>UTREQ0123456789abcdef</RequestId></Error>").getBytes("UTF-8"));
    Assert.assertNotNull("用例前提:服务端错误体能被解析", error);
    impl.sessionAttachError = new OdpsException("attach session failed", new RestException(error));

    String reason = impl.describeSessionAttachError();
    Assert.assertTrue("摘要应带 errorCode: " + reason, reason.contains("ODPS-0430015"));
    Assert.assertTrue("摘要应带 requestId: " + reason,
        reason.contains("UTREQ0123456789abcdef"));
    impl.sessionAttachError = null;
    Assert.assertNull(impl.describeSessionAttachError());
    impl.close();
  }

  /** OFFLINE 模式构造期不发 attach 请求,不应有降级原因。 */
  @Test
  public void testOfflineModeHasNoAttachError() throws OdpsException {
    SQLExecutor executor = SQLExecutorBuilder.builder()
        .odps(offlineOdps())
        .executeMode(ExecuteMode.OFFLINE)
        .useInstanceTunnel(false)
        .build();
    Assert.assertNull(((SQLExecutorImpl) executor).getSessionAttachError());
    Assert.assertNull(((SQLExecutorImpl) executor).describeSessionAttachError());
    executor.close();
  }

  /** 重复归还(池模式下 close() 等价于归还,之后再显式归还)必须是幂等的空操作。 */
  @Test
  public void testDoubleReleaseIsNoop() throws OdpsException {
    SQLExecutorPool pool = newPool(1, 2);
    SQLExecutor borrowed = pool.getExecutor();
    borrowed.close();
    pool.releaseExecutor(borrowed);
    Assert.assertEquals(1, pool.getActiveCount());
    Assert.assertEquals(0, pool.getBusyCount());
    Assert.assertEquals(1, pool.getExecutorCount());
  }

  /** 只建没跑过查询的 executor 没有 Instance,close() 不能因此抛异常,且二次 close 幂等。 */
  @Test
  public void testCloseWithoutAnyQuery() throws OdpsException {
    SQLExecutorPool pool = newPool(2, 2);
    pool.close();
    pool.close();
    Assert.assertEquals(0, pool.getActiveCount());
    Assert.assertEquals(0, pool.getBusyCount());
    Assert.assertEquals(0, pool.getExecutorCount());
  }

  /** initPoolSize 超过 maxPoolSize 时按 maxPoolSize 收敛,不能突破上限。 */
  @Test
  public void testInitPoolSizeCappedByMaxPoolSize() throws OdpsException {
    SQLExecutorPool pool = newPool(3, 2);
    Assert.assertEquals(2, pool.getExecutorCount());
    Assert.assertEquals(2, pool.getActiveCount());
    pool.close();
  }

  /** 池关闭后不应再悄悄新建连接。 */
  @Test
  public void testGetExecutorAfterCloseFailsFast() throws OdpsException {
    SQLExecutorPool pool = newPool(1, 2);
    pool.close();
    try {
      pool.getExecutor();
      Assert.fail("closed pool should not hand out executors");
    } catch (OdpsException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("closed"));
    }
  }

  /** 关闭后归还正在使用的 executor:close() 已经把它记账清掉,归还应为空操作,不能复活连接。 */
  @Test
  public void testReleaseAfterCloseIsNoop() throws OdpsException {
    SQLExecutorPool pool = newPool(1, 2);
    SQLExecutor borrowed = pool.getExecutor();
    pool.close();
    pool.releaseExecutor(borrowed);
    Assert.assertEquals(0, pool.getActiveCount());
    Assert.assertEquals(0, pool.getExecutorCount());
  }
}
