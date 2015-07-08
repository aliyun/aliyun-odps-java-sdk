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

package com.aliyun.odps.udf;

import com.aliyun.odps.io.Writable;

/**
 * <p>
 * 继承 {@link Aggregator} 实现 UDAF。<br />
 * UDAF (User Defined Aggregation Function) ：用户自定义聚合函数，其输入输出是多对一的关系，即将多条输入记录聚合成一条输出值。
 * 可以与 SQL 中的 Group By 语句联用。<br />
 *
 * <br />
 * 实现 Java UDAF 类需要继承 Aggregator 类。<br />
 * Aggregator 流程主要分为四部分，分别对应四个主要接口：
 * <ul>
 *     <li>
 *     {@link #newBuffer()} 聚合中间值 buffer 的创建和初始化。
 *     {@link #iterate(Writable, Writable[])} 实现此方法对输入数据进行计算，聚合到中间值 buffer。其中第一个参数是 newBuffer() 产生的结果，第二个参数是数据源。
 *     {@link #merge(Writable, Writable)} 实现此方法将两个中间值 merge 聚合到一起。其中第一个参数是 newBuffer() 产生的结果，第二个参数是 iterate 操作完成后产生的中间结果。
 *     {@link #terminate(Writable)} 实现此方法将 merge 操作完成后产生的中间结果转换为 ODPS SQL 基本类型。
 *     <li>
 * </ul>
 * <br />
 * 初始化流程在{@link #setup(ExecutionContext)}调用中完成，用户可重写此方法来实现一次性初始操作，例如共享资源的读取等。
 * <br />
 * 聚合过程的中间数据 buffer 类继承于 {@link Writable}, 除内建类型外，用户可继承 Writable 类实现自定义类。<br />
 * buffer 大小不应该随数据量递增，最好不要超过 2MB，否则会造成内存占用过大。<br />
 * </p>
 *
 * <p>
 * 示例代码（求平均值）：
 *<pre>
 *@Resolve({"double->double"})
 public class AggrAvg extends Aggregator {
  private static class AvgBuffer implements Writable {

    private double sum = 0;
    private long count = 0;

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeDouble(sum);
      out.writeLong(count);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
      sum = in.readDouble();
      count = in.readLong();
    }
  }

  private DoubleWritable ret = new DoubleWritable();

  @Override
  public Writable newBuffer() {
    return new AvgBuffer();
  }

  @Override
  public void iterate(Writable buffer, Writable[] args) throws UDFException {
    DoubleWritable arg = (DoubleWritable) args[0];
    AvgBuffer buf = (AvgBuffer) buffer;
    if (arg != null) {
      buf.count += 1;
      buf.sum += arg.get();
    }
  }

  @Override
  public void merge(Writable buffer, Writable partial) throws UDFException {
    AvgBuffer buf = (AvgBuffer) buffer;
    AvgBuffer p = (AvgBuffer) partial;
    buf.sum += p.sum;
    buf.count += p.count;
  }

  @Override
  public Writable terminate(Writable buffer) throws UDFException {
    AvgBuffer buf = (AvgBuffer) buffer;
    if (buf.count == 0) {
      ret.set(0);
    } else {
      ret.set(buf.sum / buf.count);
    }
    return ret;
  }
}
 *</pre>
 * </p>
 */
public abstract class Aggregator implements ContextFunction {

  /**
   * 初始化工作。包括一些共享资源的载入等。
   * 只在初始时被调用，建议一次性的操作都写入本方法。
   *
   * 资源载入通过 {@link ExecutionContext} 完成
   * @param ctx
   */

  @Override
  public void setup(ExecutionContext ctx) throws UDFException {
  }

  /**
   * 扫尾工作
   */

  @Override
  public void close() throws UDFException {
  }

  /**
   * 创建聚合Buffer
   *
   * @return Writable 聚合buffer
   */
  abstract public Writable newBuffer();

  /**
   * 对输入进行计算，生成中间结果
   * @param buffer
   *     聚合buffer
   * @param args
   *     SQL中调用UDAF时指定的参数
   * @throws UDFException
   */
  abstract public void iterate(Writable buffer, Writable[] args) throws UDFException;

  /**
   * 生成最终结果
   *
   * @param buffer
   * @return Object UDAF的最终结果
   * @throws UDFException
   */
  abstract public Writable terminate(Writable buffer) throws UDFException;

  /**
   * 聚合中间结果，将 partial merge 到 buffer
   *
   * @param buffer
   * @param partial
   * @throws UDFException
   */
  abstract public void merge(Writable buffer, Writable partial) throws UDFException;
}
