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

package com.aliyun.odps.table.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A collection of metrics.
 */
public class Metrics {

    private final Map<String, Metric> metricMap = new HashMap<>();

    public void register(Metric metric) {
        metricMap.put(metric.name(), metric);
    }

    public Optional<Metric> get(String name) {
        if (metricMap.containsKey(name)) {
            return Optional.of(metricMap.get(name));
        } else {
            return Optional.empty();
        }
    }

    public Optional<Counter> counter(String name) {
        if (metricMap.containsKey(name)) {
            return Optional.of((Counter) metricMap.get(name));
        } else {
            return Optional.empty();
        }
    }

    public <T, G extends Gauge<T>> Optional<G> gauge(String name) {
        if (metricMap.containsKey(name)) {
            return Optional.of((G) metricMap.get(name));
        } else {
            return Optional.empty();
        }
    }

    // TODO: toString
}
