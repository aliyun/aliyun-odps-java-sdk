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

package com.aliyun.odps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.aliyun.odps.account.AliyunAccount;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Offline test for the cancel path of {@link Instance#waitForTerminated(long, boolean)}: no request
 * is sent, because the poll stub returns a running instance, and the interrupt is raised while the
 * wait is sleeping.
 */
public class InstanceWaitForTerminatedInterruptTest {

  /** Never terminates through the poll loop, and records how often the loop asked for the status. */
  private static final class AlwaysRunningInstance extends Instance {

    private final AtomicInteger polls = new AtomicInteger();

    AlwaysRunningInstance(Odps odps) {
      super("test_project", new TaskStatusModel(), null, odps);
    }

    @Override
    public Status getStatus(boolean isBlock) {
      polls.incrementAndGet();
      return Status.RUNNING;
    }
  }

  @Test(timeout = 60_000)
  public void interruptDuringPollIntervalKeepsCancelSignal() throws Exception {
    Odps odps = new Odps(new AliyunAccount("test-id", "test-key"));
    odps.setEndpoint("http://127.0.0.1:1");
    odps.setDefaultProject("test_project");
    AlwaysRunningInstance instance = new AlwaysRunningInstance(odps);

    final int[] flagAfterReturn = new int[]{-1};
    final long[] elapsedMs = new long[1];
    Thread worker = new Thread(() -> {
      long start = System.currentTimeMillis();
      instance.waitForTerminated(20_000, false);
      elapsedMs[0] = System.currentTimeMillis() - start;
      flagAfterReturn[0] = Thread.currentThread().isInterrupted() ? 1 : 0;
    }, "instance-wait-cancel-test");
    worker.setDaemon(true);
    worker.start();

    long deadline = System.currentTimeMillis() + 10_000;
    while (instance.polls.get() == 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    worker.interrupt();
    worker.join(30_000);

    assertFalse("waitForTerminated did not return after the interrupt", worker.isAlive());
    assertEquals("the 20s poll interval must not be slept to the end after a cancel", true,
        elapsedMs[0] < 10_000);
    // waitForTerminated ends without throwing, so the interrupt flag is the only thing that can
    // report the cancel to the caller.
    assertEquals("interrupt status must survive waitForTerminated", 1, flagAfterReturn[0]);
    // do not leak the flag into the next test on this thread
    Thread.interrupted();
  }
}
