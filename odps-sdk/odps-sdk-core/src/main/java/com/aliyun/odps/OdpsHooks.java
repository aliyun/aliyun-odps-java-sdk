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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * OdpsHooks 类
 * 注册 OdpsHook 类 用于在启动 Instance 前后分别运行插件代码
 *
 * 一个 OdpsHooks 实例针对一个 Instance， 包含了一组实例化的 OdpsHook 对象
 */
public class OdpsHooks {


  private static boolean enabled = true;

  private static List<Class<? extends OdpsHook>>
      registeredHooks =
      new ArrayList<Class<? extends OdpsHook>>();

  private List<OdpsHook> runningHooks = new ArrayList<OdpsHook>();

  private static final Object lock = new Object();

  /**
   * 新建一个 OdpsHooks 对象
   */
  public OdpsHooks() {
    try {
      synchronized (lock) {
        for (Class<? extends OdpsHook> hookClass : registeredHooks) {
          runningHooks.add(hookClass.newInstance());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * 调用 Hook 的 before
   *
   * @param job
   *     调用 hook 的 job
   * @param odps
   *     odps 实例
   * @throws OdpsException
   */
  public void before(Job job, Odps odps) throws OdpsException {
    for (OdpsHook hook : runningHooks) {
      hook.before(job, odps);
    }
  }

  /**
   * 调用 Hook 的 ready
   *
   * @param instance
   *     创建的 instance
   * @param odps
   *     odps 实例
   * @throws OdpsException
   */
  public void onInstanceCreated(Instance instance, Odps odps) throws OdpsException {
    for (OdpsHook hook : runningHooks) {
      hook.onInstanceCreated(instance, odps);
    }
  }

  /**
   * 调用 Hook 的 after
   *
   * @param instance
   *     创建的 instance
   * @param odps
   *     odps 实例
   * @throws OdpsException
   */
  public void after(Instance instance, Odps odps) throws OdpsException {
    for (OdpsHook hook : runningHooks) {
      hook.after(instance, odps);
    }
  }

  /**
   * 注册多个 hook
   *
   * @param hooks
   *     注册的 hook 列表
   */
  public static void registerHooks(Collection<Class<? extends OdpsHook>> hooks) {
    synchronized (lock) {
      registeredHooks.addAll(hooks);
    }
  }

  /**
   * 注册一个 hook
   *
   * @param hook
   *     注册的 hook 名字列表
   */
  public static void registerHook(Class<? extends OdpsHook> hook) {
    synchronized (lock) {
      registeredHooks.add(hook);
    }
  }

  @Deprecated
  public static List<Class<? extends OdpsHook>> getRegisteredHooks() {
    return registeredHooks;
  }

  /**
   * 清空 hooks
   */
  public static void clearRegisteredHooks() {
    synchronized (lock) {
      registeredHooks.clear();
    }
  }

  /**
   * 获取 hook 开关
   *
   * @return 是否开启 hook
   */
  public static boolean isEnabled() {
    return enabled;
  }

  /**
   * 设置 hook 开关
   *
   * @param enabled
   *     是否开启 hook
   */
  public static void setEnabled(boolean enabled) {
    OdpsHooks.enabled = enabled;
  }

}
