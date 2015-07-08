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

import java.io.BufferedOutputStream;
import java.io.IOException;

/**
 * OutputBlobWritable表示ODPS表或分区中列类型为blob的输出接口, 该接口只能用于
 * {@link com.aliyun.odps.mapreduce.Reducer}向 {@link com.aliyun.odps.Record}
 * 中写入blob类型的列.
 * <p>
 * 用法示例：
 *
 * <pre>
 * &#064;Override
 * public void reduce(KEYIN key, Iterable&lt;VALUEIN&gt; values,
 *     MapContext&lt;KEYIN, VALUEIN&gt; context) throws IOException {
 *   Record outputRecord = context.createOutputRecord();
 *   OutputBlobWritable blob = outputRecord.get(0);
 *   BufferedOutputStream out = blob.getOutputStream();
 *   // 使用out.write向blob中写入数据
 *   out.close();
 * }
 * </pre>
 *
 * 从上面的示例看出, 从{@link com.aliyun.odps.Record}获得OutputBlobWritable接口,
 * 调用OutputBlobWritable的 getOutputStream获得BufferedOutputStream后，即可进行写入数据.
 * </p>
 */
public interface OutputBlobWritable extends WritableComparable {

  /**
   * getOutputStream方法获取可以写入blob的{@link BufferedOutputStream}接口
   *
   * @return blob列对应的{@link BufferedOutputStream}
   * @throws IOException
   */
  public BufferedOutputStream getOutputStream() throws IOException;
}
