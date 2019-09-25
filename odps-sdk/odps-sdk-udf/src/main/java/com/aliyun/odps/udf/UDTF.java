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

import com.aliyun.odps.type.TypeInfo;
import java.io.IOException;

/**
 * {@link UDTF} 是 User Defined Table-generating Function 缩写，用来解决一次函数调用输出多行数据的场景，也是唯一能返回多个字段的自定义函数。<br />
 * <p>
 * 用户代码需继承此类，编写 {@link #process(Object[])} 方法来完成具体的数据操作，实现一个 {@link UDTF} 的功能。<br />
 * 行数据输出的过程通过调用 {@link #forward(Object...)} 实现。
 * </p>
 * <br />
 * 实例代码：
 * <pre>
 *@Resolve({ "string,string->string,bigint" })
  public class UDTFExample extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException {
      String a = (String) args[0];
      long b = args[1] == null ? 0 : ((String)args[1]).length();

      forward(a, b);
    }
  }
 * </pre>
 */
public abstract class UDTF implements ContextFunction {

  private UDTFCollector collector;

  /**
   * 不推荐使用
   * 用于输入、输出类型 {@link OdpsType} 之间的转换
   *
   * @param inputTypes
   *     输入的类型
   * @return 转换后的输出类型
   * @throws Exception
   *     任何异常均会抛出
   *
   *     建议使用 {@link Resolve} 替代
   */
  @Deprecated
  public OdpsType[] initialize(OdpsType[] inputTypes) throws Exception {
    OdpsType[] ret = {OdpsType.IGNORE};
    return ret;
  }

  /**
   * 用于输入、输出类型 {@link OdpsType} 之间的转换，默认调用 {@link #initialize(OdpsType[])}
   * 不存在转换问题通常不需要关心。
   *
   * @param inputTypes
   *     输入的类型
   * @return 转换后的输出类型
   * @throws UDFException
   *     通用{@link UDF}异常
   */
  @Deprecated
  public OdpsType[] resolve(OdpsType[] inputTypes) throws UDFException {
    try {
      return initialize(inputTypes);
    } catch (Exception e) {
      throw new UDFException(e);
    }
  }

  /**
   * 用于输入、输出类型 {@link TypeInfo} 之间的转换，默认调用 {@link #resolve(OdpsType[])}
   * 不存在转换问题通常不需要关心。
   *
   * @param typeInfos
   *     输入的类型
   * @return 转换后的输出类型
   * @throws UDFException
   *     通用{@link UDF}异常
   */
  public TypeInfo[] resolve(TypeInfo[] typeInfos) throws UDFException {
    return null;
  }

  /**
   * 在{@link UDTF}运行期，在每个Worker内{@link #setup(ExecutionContext)}会被先调用一次。
   * <p>
   * 默认实现为空实现，用户代码可以通过覆盖此方法，在{@link #setup(ExecutionContext)}中完成初始化的工作。
   * </p>
   *
   * @param ctx
   *     {@link UDF}所在运行时上下文信息，包括{@link UDF}所在的StageID、WorkerID等信息
   * @throws UDFException
   *     通用{@link UDF}异常
   */
  public void setup(ExecutionContext ctx) throws UDFException {
  }

  /**
   * 用户代码必须实现{@link #process(Object[])}方法。
   * <p>
   * 对SQL中每一条记录，{@link #process(Object[])}方法都会被调用一次。
   * </p>
   *
   * @param args
   *     参数数组，对应SQL调用时的参数。
   * @throws UDFException
   *     通用{@link UDF}异常
   */
  public abstract void process(Object[] args) throws UDFException,IOException;

  /**
   * 在{@link UDTF}运行期，{@link #process(Object[])}把全部记录处理完以之后，
   * 即当前实例没有其它需要处理的记录了，{@link #close()}}此时会被调用一次。
   * <p>
   * 默认实现为空实现，用户代码可以覆盖此方法。
   * </p>
   *
   * @throws UDFException
   *     通用{@link UDF}异常
   */
  public void close() throws UDFException {

  }

  /**
   * UDTF输出，对应SQL语句UDTF的as子句指定的列。<br />
   * 该方法通常是把输出行传给关联的 {@link UDTFCollector} 实例。
   *
   * <p>
   * 默认实现调用 {@link UDTFCollector#collect(Object[])}
   * </p>
   * 比如：</br>
   * <pre>
   * {@code
   *   forward(new String("Hello"), new Long(1), new Double(1.0), new Boolean(true));
   * }
   * </pre>
   * 分别输出"Hello",1,1.0,true
   *
   * @param outs
   *     {@link UDTF}输入参数构成的对象数组
   * @throws UDFException
   *     通用{@link UDF}异常
   */
  protected void forward(Object... outs) throws UDFException {
    collector.collect(outs);
  }

  /**
   * 设置 {@link UDTFCollector}，以便该 {@link UDTFCollector} 对象与 {@link UDTF} 实例相关联。 <br />
   * 由于 {@link UDTF} 可能会在 {@link UDTFCollector} 构造之前被初始化，所以不能在 {@link UDTF}
   * 的构造函数中指定 {@link UDTFCollector} 。
   *
   * <p>
   * 设置一个实现UDTFCollector接口的对象。注意：如果在运行期户代码通过调用此方法覆盖默认的UDTFCollector对象，将会造成UDTF无法正常输出。
   * </p>
   *
   * @param collector
   *     一个实现了UDTFCollector接口的对象。
   * @see UDTFCollector
   */
  public void setCollector(UDTFCollector collector) {
    this.collector = collector;
  }

  /**
   * 获取{@link UDTF}的默认{@link UDTFCollector}对象。
   * <p>
   * {@link UDTF}在运行时，会被初始化一个实现了{@link UDTFCollector}接口的对象，用户代码通过 {@link #forward(Object...)}
   * 间接调用 {@link UDTFCollector#collect(Object[])}，将结果输出。
   * </p>
   *
   * @return 一个初始化好的，实现了{@link UDTFCollector}接口的对象。
   * @see UDTFCollector
   */
  public UDTFCollector getCollector() {
    return collector;
  }
}
