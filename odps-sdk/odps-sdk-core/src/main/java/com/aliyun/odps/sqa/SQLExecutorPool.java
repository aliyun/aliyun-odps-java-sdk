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

import com.aliyun.odps.OdpsException;

import java.util.*;

public class SQLExecutorPool {
  private SQLExecutorBuilder sqlExecutorBuilder = null;
  private int initCount = 1;
  private int currentCount = 0;
  private int maxCount = 1;
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
      while (initCount > currentCount) {
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
    synchronized (this) {
      Iterator<Map.Entry<String, SQLExecutor>> it = busyExecutor.entrySet().iterator();
      while(it.hasNext()) {
        Map.Entry<String, SQLExecutor> entry = it.next();
        try {
          entry.getValue().getInstance().stop();
        } catch (OdpsException e) {
          // ignore
        }
      }
      while (!activeExecutor.isEmpty()) {
        SQLExecutor sqlExecutor = activeExecutor.poll();
        try {
          sqlExecutor.getInstance().stop();
        } catch (OdpsException e) {
          // ignore
        }
      }
      currentCount = 0;
      activeExecutor.clear();
      busyExecutor.clear();
    }
  }

  /**
   * 获取一个连接,在最大连接数限制内会进行初始化动作
   * @return 一个可用的链接
   * @throws OdpsException
   */
  public SQLExecutor getExecutor() throws OdpsException {
    synchronized (this) {
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
    synchronized (this) {
      if (executor != null) {
        if (busyExecutor.containsKey(executor.getId())) {
          SQLExecutor sqlExecutor = busyExecutor.remove(executor.getId());
          if (sqlExecutor.isActive()) {
            activeExecutor.add(sqlExecutor);
          } else {
            currentCount--;
          }
        }
      }
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