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

package com.aliyun.odps.data;

import java.util.Objects;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/**
 * 基于 JTS 的 GeographyObject 实现。
 */
public final class JtsGeographyObject implements GeographyObject {

  private static final long serialVersionUID = 1L;

  private static final WKBWriter WKB_WRITER = new WKBWriter();
  private static final WKBReader WKB_READER = new WKBReader();
  private static final WKTWriter WKT_WRITER = new WKTWriter();
  private static final WKTReader WKT_READER = new WKTReader();

  static {
    WKT_WRITER.setPrecisionModel(new PrecisionModel(1e12));
  }

  /** 可能为 null：如果是从 WKT 构造的，且还没写过 WKB */
  private transient volatile byte[] cachedWkb;
  /** 可能为 null：如果是从 WKB 构造的，且还没写过 WKT */
  private transient volatile String cachedWkt;
  /** 可能为 null，按需从 WKB/WKT 解析 */
  private transient volatile Geometry geometry;

  private JtsGeographyObject(byte[] wkb, String wkt, Geometry geometry) {
    this.cachedWkb = wkb == null ? null : wkb.clone();
    this.cachedWkt = wkt;
    this.geometry = geometry;
  }

  public static JtsGeographyObject fromWkb(byte[] wkb) {
    Objects.requireNonNull(wkb, "wkb must not be null");
    // 一开始只保存 WKB，不解析 Geometry
    return new JtsGeographyObject(wkb, null, null);
  }

  public static JtsGeographyObject fromWkt(String wkt) {
    Objects.requireNonNull(wkt, "wkt must not be null");
    // 一开始只保存 WKT，不解析 Geometry
    return new JtsGeographyObject(null, wkt, null);
  }

  public static JtsGeographyObject fromGeometry(Geometry geometry) {
    Objects.requireNonNull(geometry, "geometry must not be null");
    return new JtsGeographyObject(null, null, geometry);
  }

  public Geometry getGeometry() {
    Geometry g = geometry;
    if (g == null) {
      synchronized (this) {
        g = geometry;
        if (g == null) {
          try {
            if (cachedWkb != null) {
              g = WKB_READER.read(cachedWkb);
            } else if (cachedWkt != null) {
              g = WKT_READER.read(cachedWkt);
            } else {
              throw new IllegalStateException("No source (WKB/WKT/Geometry) to create Geometry");
            }
            geometry = g;
          } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse geometry", e);
          }
        }
      }
    }
    return g;
  }

  @Override
  public byte[] asBinary() {
    byte[] wkb = cachedWkb;
    if (wkb == null) {
      synchronized (this) {
        wkb = cachedWkb;
        if (wkb == null) {
          wkb = WKB_WRITER.write(getGeometry());
          cachedWkb = wkb;
        }
      }
    }
    return wkb.clone();
  }

  @Override
  public String asText() {
    String wkt = cachedWkt;
    if (wkt == null) {
      synchronized (this) {
        wkt = cachedWkt;
        if (wkt == null) {
          wkt = WKT_WRITER.write(getGeometry());
          cachedWkt = wkt;
        }
      }
    }
    return wkt;
  }

  @Override
  public String toString() {
    return asText();
  }
}

