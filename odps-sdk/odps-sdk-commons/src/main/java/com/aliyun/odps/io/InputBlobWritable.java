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

package com.aliyun.odps.io;

import java.io.IOException;

/**
 * InputBlobWritable表示ODPS表或分区中列类型为blob的输入接口. 该接口只能用于
 * {@link com.aliyun.odps.mapreduce.Mapper}从 {@link com.aliyun.odps.Record}
 * 中读取blob类型的列.
 * <p>
 * 用法示例：
 *
 * <pre>
 * &#064;Override
 * public void map(LongWritable key, Record value,
 *     MapContext&lt;KEYOUT, VALUEOUT&gt; context) throws IOException {
 *   InputBlobWritable blob = value.get(0);
 *   BlobDataInputStream in = blob.getInputStream();
 *   long length = in.getLength();
 *   // 使用in.read从blob中读取
 *   in.close();
 * }
 * </pre>
 *
 * 从上面的示例看出, 从{@link com.aliyun.odps.Record}获得InputBlobWritable接口,
 * 调用InputBlobWritable的getInputStream获得BlobDataInputStream后, 即可读出数据.
 * </p>
 */
public interface InputBlobWritable extends WritableComparable {

  /**
   * getInputStream方法获取可以读取blob的{@link BlobDataInputStream}接口
   *
   * @return blob列对应的{@link BlobDataInputStream}
   * @throws IOException
   */
  public BlobDataInputStream getInputStream() throws IOException;

  /**
   * getLength方法获取blob的总字节数
   *
   * @return blob列的字节数
   * @throws IOException
   */
  public long getLength() throws IOException;

}
