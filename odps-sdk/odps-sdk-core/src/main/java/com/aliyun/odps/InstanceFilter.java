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

import java.util.Date;

import com.aliyun.odps.Instance.Status;

/**
 * InstanceFilter用于查询所有{@link Instance}时根据条件过滤
 *
 * <p>
 *
 * 例如:<br />
 * <pre>
 * <code>
 * InstanceFilter filter = new InstanceFilter();
 * filter.setFromTime(fromTime);
 * filter.setEndTime(endTime);
 * filter.setStatus(Instance.Status.TERMINATED);
 *
 * for (Instance i : odps.instances().iterator(filter)) {
 *     // do somthing on the Instance object
 * }
 * </code>
 * </pre>
 * </p>
 */
public class InstanceFilter {

  private Date fromTime;

  private Date endTime;

  private Status status;

  private Boolean onlyOwner;

  /**
   * 获得起始执行时间过滤条件
   *
   * @return 起始时间
   */
  public Date getFromTime() {
    return fromTime;
  }

  /**
   * 设置起始时间过滤条件
   *
   * @param fromTime
   *     起始时间
   */
  public void setFromTime(Date fromTime) {
    this.fromTime = fromTime;
  }

  /**
   * 获得结束时间的过滤条件
   *
   * @return 结束时间
   */
  public Date getEndTime() {
    return endTime;
  }

  /**
   * 设置结束时间的过滤条件
   *
   * @param endTime
   *     结束时间
   */
  public void setEndTime(Date endTime) {
    this.endTime = endTime;
  }

  /**
   * 获得{@link Instance}状态过滤条件
   *
   * @return {@link Instance}状态 {@link Status}
   */
  public Status getStatus() {
    return status;
  }

  /**
   * 设置{@link Instance}状态过滤条件，只返回处于该状态的{@link Instance}
   *
   * @param status
   *     Instance状态 {@link Status}
   */
  public void setStatus(Status status) {
    this.status = status;
  }

  /**
   * 获得{@link Instance} owner过滤条件
   *
   * @param onlyOwner
   *     只查询当前用户作为owner的Instance
   */
  public Boolean getOnlyOwner() {
    return onlyOwner;
  }

  /**
   * 设置{@link Instance}状态过滤条件，只返回owner的{@link Instance}
   *
   * @param onlyOwner
   *     只查询当前用户作为的Instance
   */
  public void setOnlyOwner(Boolean onlyOwner) {
    this.onlyOwner = onlyOwner;
  }
}
