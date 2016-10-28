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

package com.aliyun.odps.mapred.bridge.utils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.cache.DistributedCache;
import com.aliyun.odps.data.Record;

@SuppressWarnings("deprecation")
public final class ResourceUtils {

  /**
   * 读取一个名为resourceName的文件资源.
   *
   * @param resourceName
   *     资源名称
   * @return BufferedInputStream 输入流
   * @throws IOException
   */
  public static BufferedInputStream readResourceFileAsStream(String resourceName)
      throws IOException {
    return DistributedCache.readCacheFileAsStream(resourceName);
  }
  
  /**
   * 读取压缩档案类型资源，返回 BufferedInputStream 的迭代器.
   * 
   * @see #readCacheArchive(String)
   * @see DistributedCache#readCacheArchiveAsStream(String)
   * 
   * @param resourceName
   *          资源名称
   * @return BufferedInputStream的迭代器
   * @throws IOException
   *           资源未声明、资源类型不匹配以及其他读取错误抛异常
   */
  public static Iterable<BufferedInputStream> readResourceArchiveFileAsStream(String resourceName) throws IOException {
    return DistributedCache.readCacheArchiveAsStream(resourceName);
  }
  
  /**
   * 读取压缩档案类型资源，返回 BufferedInputStream 的迭代器.
   * 
   * @see #readCacheArchive(String, String)
   * @see DistributedCache#readCacheArchiveAsStream(String, String)
   * 
   * @param resourceName
   *          资源名称
   * @param relativePath
   *          读取资源的相对路径
   * @return BufferedInputStream的迭代器
   * @throws IOException
   *           资源未声明、资源类型不匹配以及其他读取错误抛异常
   */
  public static Iterable<BufferedInputStream> readResourceArchiveFileAsStream(String resourceName, String relativePath)
      throws IOException {
    return DistributedCache.readCacheArchiveAsStream(resourceName, relativePath);
  }

  /**
   * 读取一个名为resourceName的资源表
   *
   * @param resourceName
   *     资源名称
   * @return {@link Record}的迭代器
   * @throws IOException
   */
  public static Iterator<Record> readResourceTable(String resourceName)
      throws IOException {
    final Iterator<com.aliyun.odps.Record> itr = DistributedCache
        .readCacheTable(resourceName).iterator();
    return new Iterator<Record>() {

      @Override
      public boolean hasNext() {
        return itr.hasNext();
      }

      @Override
      public Record next() {
        com.aliyun.odps.Record r = itr.next();
        return VersionUtils.adaptRecord(r);
      }

      @Override
      public void remove() {
        itr.remove();
      }

    };

  }
}
