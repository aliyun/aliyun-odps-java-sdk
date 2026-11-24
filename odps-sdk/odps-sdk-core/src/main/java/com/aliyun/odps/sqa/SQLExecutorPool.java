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

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SQLExecutorPool {
  private static final Logger LOG = LoggerFactory.getLogger(SQLExecutorPool.class);

  private SQLExecutorBuilder sqlExecutorBuilder = null;
  private int initCount = 1;
  private int currentCount = 0;
  private int maxCount = 1;
  private boolean closed = false;
  private Queue<SQLExecutor> activeExecutor = new ArrayDeque<>();
  private Map<String, SQLExecutor> busyExecutor = new HashMap<>();

  static SQLExecutorPool create(int initCount, int maxCount, SQLExecutorBuilder sqlExecutorBuilder) throws OdpsException {
    return new SQLExecutorPool(initCount, maxCount, sqlExecutorBuilder);
  }

  private SQLExecutorPool(int initCount, int maxCount, SQLExecutorBuilder sqlExecutorBuilder) throws OdpsException {
    this.initCount = initCount;
    this.maxCount = maxCount;
    this.sqlExecutorBuilder = sqlExecutorBuilder.setPool(this);
    init();
  }

  /**
   * 初始化连接池,将会初始化initCount配置的连接数
   * @return
   * @throws OdpsException
   */
  private void init() throws OdpsException {
    synchronized (this) {
      while (currentCount < maxCount && currentCount < initCount) {
        // init new session
        SQLExecutor sqlExecutor = sqlExecutorBuilder.build();
        currentCount++;
        activeExecutor.add(sqlExecutor);
      }
    }
  }

  /**
   * 关闭连接池
   * @return
   * @throws
   */
  public void close() {
    List<SQLExecutor> executors;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      executors = new ArrayList<>(activeExecutor);
      executors.addAll(busyExecutor.values());
      activeExecutor.clear();
      busyExecutor.clear();
      currentCount = 0;
    }
    // stop() 是网络调用,放在锁外,避免关闭期间阻塞 getExecutor()/releaseExecutor()
    for (SQLExecutor executor : executors) {
      stopQuietly(executor);
    }
  }

  /**
   * 获取一个连接,在最大连接数限制内会进行初始化动作
   * @return 一个可用的链接
   * @throws OdpsException
   */
  public SQLExecutor getExecutor() throws OdpsException {
    synchronized (this) {
      if (closed) {
        throw new OdpsException("Executor pool is closed.");
      }
      SQLExecutor sqlExecutor = null;
      if (!activeExecutor.isEmpty()) {
        sqlExecutor = activeExecutor.poll();
        if (sqlExecutor != null) {
          busyExecutor.put(sqlExecutor.getId(), sqlExecutor);
        }
        return sqlExecutor;
      } else if (currentCount < maxCount) {
        // create new session
        sqlExecutor = sqlExecutorBuilder.build();
        currentCount++;
        busyExecutor.put(sqlExecutor.getId(), sqlExecutor);
      } else {
        throw new OdpsException("No active executor in pool.");
      }
      return sqlExecutor;
    }
  }

  /**
   * 释放一个连接,将其放回可用池
   * @return
   * @throws
   */
  public void releaseExecutor(SQLExecutor executor) {
    if (executor == null) {
      return;
    }
    SQLExecutor evicted = null;
    synchronized (this) {
      SQLExecutor sqlExecutor = busyExecutor.remove(executor.getId());
      if (sqlExecutor == null) {
        // 不属于本池,或者已经归还过(SQLExecutor#close() 在池模式下等价于归还),保持幂等
        return;
      }
      if (!closed && !evictedAsUnusable(sqlExecutor)) {
        activeExecutor.add(sqlExecutor);
        return;
      }
      // 淘汰的连接要真正停掉,否则它的 MCQA session 会一直挂在服务端
      currentCount--;
      evicted = sqlExecutor;
      // 淘汰后补回保底容量,否则池会随着 session 抖动单向缩小,借出延迟转嫁给调用方
      while (currentCount < initCount && currentCount < maxCount) {
        try {
          SQLExecutor refill = sqlExecutorBuilder.build();
          currentCount++;
          activeExecutor.add(refill);
        } catch (OdpsException e) {
          LOG.warn("fail to refill executor after eviction: {}", e.getMessage());
          break;
        }
      }
    }
    stopQuietly(evicted);
  }

  /**
   * 归还的 executor 是否可以直接回到空闲队列。
   *
   * executeMode=OFFLINE 的 executor 不持有 session,isActive() 恒为 false,但它是可复用的;
   * attach 失败降级的 executor 同理(它的 session 从未建立)。判据是"有没有 session",
   * 不是"创建时请求了哪种模式"——只有真持有 session 且 session 已死的才需要淘汰。
   * 池已关闭时一律淘汰。
   */
  private boolean evictedAsUnusable(SQLExecutor executor) {
    if (closed) {
      return true;
    }
    SQLExecutorImpl impl = executor instanceof SQLExecutorImpl ? (SQLExecutorImpl) executor : null;
    if (impl != null && impl.getCreatedExecuteMode() == ExecuteMode.OFFLINE) {
      return false;
    }
    // 没有 session 就没有可泄漏的东西:attach 失败降级的 executor 与 OFFLINE 模式同构,
    // 淘汰它既不释放任何服务端资源,又让 releaseExecutor 在池锁内多做一次带网络的 build()
    // (Session.attach 受 attachTimeout 约束)。MCQA 故障期这会把每次归还变成阻塞整个池锁的
    // 重试风暴,真实回归 SQLExecutorTest#testExecutorPoolNormal 也正是断在这里。
    if (impl != null && !impl.holdsSession()) {
      return false;
    }
    if (executor.isActive()) {
      return false;
    }
    // 淘汰是有代价的:池会缩小、session 会被 stop。不打原因,"池子越用越小"就无从查起。
    String attachReason = impl == null ? "unknown"
        : String.valueOf(impl.describeSessionAttachError());
    LOG.warn("evicting executor on release: id={}, createdExecuteMode={}, attachError={}",
        executor.getId(),
        impl == null ? "unknown" : impl.getCreatedExecuteMode(),
        attachReason,
        impl == null ? null : impl.getSessionAttachError());
    return true;
  }

  /**
   * 停掉一个 executor 持有的 instance。未执行过查询且没有 session 的 executor 没有 instance,
   * 不能直接 getInstance().stop()。
   */
  private static void stopQuietly(SQLExecutor executor) {
    try {
      Instance instance = executor.getInstance();
      if (instance != null) {
        instance.stop();
      }
    } catch (Exception e) {
      LOG.warn("failed to stop executor {}: {}", executor.getId(), e.getMessage());
    }
  }

  public int getActiveCount() {
    synchronized (this) {
      return activeExecutor.size();
    }
  }

  public int getBusyCount() {
    synchronized (this) {
      return busyExecutor.size();
    }
  }

  public int getExecutorCount() {
    synchronized (this) {
      return currentCount;
    }
  }
}