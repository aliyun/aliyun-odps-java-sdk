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

package com.aliyun.odps.utils;

import com.aliyun.odps.commons.transport.Connection;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class ConnectionWatcher {
  public class Timeout {
    private long startTime;
    private long expectLasts;
    private boolean timedAlready;

    private int retriedTimes;
    private static final int MAX_RETRY_TIMES = 5;

    public Timeout() { this(0, 0); }
    public Timeout(long expectLs) {
      this(System.currentTimeMillis(), expectLs);
    }
    public Timeout(long startTm, long expectLs) {
      startTime = startTm;
      expectLasts = expectLs;
      timedAlready = false;
      retriedTimes = 0;
    }
    public boolean timedOut() {
      long current = System.currentTimeMillis();
      if (current - startTime > expectLasts) {
        return true;
      }
      return false;
    }
    /**
     * while keeping the expectLasts, reset the startTime to current time.
     * timedAlready will be reset too.
     */
    public void resetTimer() {
      startTime = System.currentTimeMillis();
      timedAlready = false;
    }

    public long getStartTime() {
      return startTime;
    }

    public void setStartTime(long startTime) {
      this.startTime = startTime;
    }

    public long getExpectLasts() {
      return expectLasts;
    }

    public void setExpectLasts(long expectLasts) {
      this.expectLasts = expectLasts;
    }

    public boolean isTimedAlready() {
      return timedAlready;
    }

    public void setTimedAlready(boolean timedAlready) {
      this.timedAlready = timedAlready;
    }

    public boolean startRetry() {
      if (retriedTimes >= MAX_RETRY_TIMES) {
        return false;
      }
      retriedTimes++;
      return true;
    }

    public int getRetriedTimes() {
      return retriedTimes;
    }
  }
  private static ConnectionWatcher INSTANCE = null;
  private final HashMap<Connection, Timeout> writerTimestamp;

  private static final Logger LOG = Logger.getLogger(ConnectionWatcher.class.getCanonicalName());

  private ConnectionWatcher() {
    writerTimestamp = new HashMap<>();
    new Timer().scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        synchronized (writerTimestamp) {
          if (writerTimestamp.isEmpty()) {
            return;
          }
          Iterator<Map.Entry<Connection, Timeout>> iter = writerTimestamp.entrySet().iterator();
          while (iter.hasNext()) {
            Map.Entry<Connection, Timeout> entry = iter.next();
            Connection conn = entry.getKey();
            Timeout v = entry.getValue();
            if (!v.isTimedAlready() && v.timedOut()) {
              try {
                conn.disconnect();
                v.setTimedAlready(true);
              } catch (IOException e) {
                LOG.warning("Disconnecting timed out connection failed: retried " + v.getRetriedTimes() + " exception " + e.getMessage());
                if (!v.startRetry()) {
                  v.setTimedAlready(true);
                }
              }
            }
          }
        }
      }
    }, 1000L, 1000L);
  }

  /**
   * add the connection to Watcher.
   * @param expectLasts in millis.
   */
  public void mark(Connection conn, long expectLasts) {
    synchronized (writerTimestamp) {
      writerTimestamp.put(conn, new Timeout(expectLasts));
    }
  }

  public boolean checkTimedOut(Connection conn) {
    synchronized (writerTimestamp) {
      Timeout timeout = writerTimestamp.getOrDefault(conn, null);
      if (timeout == null || !timeout.timedOut()) {
        return false;
      }
      return true;
    }
  }

  /**
   * reset the timer of given connection.
   */
  public void refresh(Connection conn) {
    synchronized (writerTimestamp) {
      if(writerTimestamp.containsKey(conn)) {
        writerTimestamp.get(conn).resetTimer();
      }
    }
  }

  /**
   * remove the connection from watcher.
   */
  public void release(Connection conn) {
    synchronized (writerTimestamp) {
      writerTimestamp.remove(conn);
    }
  }

  public static synchronized ConnectionWatcher getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new ConnectionWatcher();
    }
    return INSTANCE;
  }
}
