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

package com.aliyun.odps.lot.operators;

import java.util.List;

import com.aliyun.odps.lot.common.ArgumentNullException;
import com.aliyun.odps.lot.common.Reference;

import apsara.odps.OrderProtos;
import apsara.odps.lot.Lot;
import apsara.odps.lot.SortByProtos;

public class SortBy extends Operator {

  public static class Order {

    private Reference reference;
    private boolean asc = true;

    public Order(Reference reference) {
      if (reference == null) {
        throw new ArgumentNullException("reference");
      }

      this.reference = reference;
    }

    public Reference getReference() {
      return reference;
    }

    public boolean isAsc() {
      return asc;
    }

    public Order(Reference reference, boolean asc) {
      if (reference == null) {
        throw new ArgumentNullException("reference");
      }

      this.reference = reference;
      this.asc = asc;

    }

    public OrderProtos.Order toProtoBuf() {
      OrderProtos.Order.Builder builder = OrderProtos.Order.newBuilder();
      builder.setAsc(asc);
      builder.setColumn(reference.toProtoBuf().getReference());
      return builder.build();
    }
  }

  private List<Order> orders;
  private boolean partial = true;

  public SortBy(List<Order> orders) {
    if (orders == null) {
      throw new ArgumentNullException("orders");
    }

    this.orders = orders;
  }

  public SortBy(List<Order> orders, boolean partial) {
    if (orders == null) {
      throw new ArgumentNullException("orders");
    }

    this.orders = orders;
    this.partial = partial;
  }

  @Override
  public Lot.LogicalOperator toProtoBuf() {
    assert (getParents().size() == 1);

    Lot.LogicalOperator.Builder builder = Lot.LogicalOperator.newBuilder();

    SortByProtos.SortBy.Builder sb = SortByProtos.SortBy.newBuilder();
    sb.setId(getId());
    sb.setParentId(getParents().get(0).getId());
    for (Order order : orders) {
      sb.addOrders(order.toProtoBuf());
    }
    builder.setSortBy(sb.build());

    return builder.build();
  }
}
