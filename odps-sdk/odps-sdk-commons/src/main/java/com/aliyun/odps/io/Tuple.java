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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.odps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.aliyun.odps.utils.CommonUtils;
import com.aliyun.odps.utils.ReflectionUtils;

/**
 * Tuple 是 {@link Writable} 对象的有序集.
 *
 * <p>
 * Tuple 将 {@link Writable} 对象保存在一个 {@link List} 中，每个 {@link Writable} 对象都是一个
 * Tuple域 （Field）。<br/>
 * Tuple 的方法与 {@link Record} 有些类似，不同之处：
 * <ul>
 * <li>Tuple 不含 schema 信息，只是数据的集合。
 * <li>Tuple 可以随意创建，{@link Record} 由 MapReduce 框架创建。
 * </ul>
 *
 * @see TupleReaderWriter
 */
public class Tuple implements WritableComparable<Object> {

  /**
   * 保存 Tuple 所有 Fields.
   */
  protected List<Writable> mFields;

  /**
   * {@link #toDelimitedString()} 使用的默认列分隔符：CTRL + A
   */
  public final static char DELIM = '\u0001';

  /**
   * {@link #toDelimitedString()} 使用的默认NULL指示符: \N
   */
  public final static String NULLINDICATOR = "\\N";

  /**
   * 默认构造函数
   */
  public Tuple() {
    mFields = new ArrayList<Writable>();
  }

  /**
   * 构造一个指定大小的 Tuple.
   *
   * @param size
   *     {@link Writable} 对象个数
   */
  public Tuple(int size) {
    mFields = new ArrayList<Writable>(size);
    for (int i = 0; i < size; i++) {
      mFields.add(null);
    }
  }

  /**
   * 给定 {@link Writable} 数组构造 Tuple.
   *
   * @param fields
   *     {@link Writable} 数组
   */
  public Tuple(Writable[] fields) {
    mFields = new ArrayList<Writable>(fields.length);
    for (Writable field : fields) {
      mFields.add(field);
    }
  }

  /**
   * 给定 {@link List} 构造 Tuple.
   *
   * @param vals
   * @param reference
   *     如果为true，新创建的 Tuple 直接使用 vals 保存 Fields，否则，会创建一个新的 {@link List}.
   */
  public Tuple(List<Writable> vals, boolean reference) {
    if (!reference) {
      Tuple temp = new Tuple(vals.toArray(new Writable[vals.size()])).clone();
      this.mFields = temp.mFields;
    } else {
      this.mFields = vals;
    }
  }

  /**
   * 复制另一个 Tuple 的数据
   *
   * @param tuple
   *     待复制的 Tuple 对象
   * @param reference
   *     如果为true，共享 {@link List}.
   */
  public Tuple(Tuple tuple, boolean reference) {
    if (!reference) {
      this.mFields = tuple.clone().mFields;
    } else {
      this.mFields = tuple.mFields;
    }
  }

  /**
   * Find the size of the tuple. Used to be called arity().
   *
   * @return number of fields in the tuple.
   */
  public int size() {
    return mFields.size();
  }

  /**
   * Get the value in a given field.
   *
   * @param fieldNum
   *     Number of the field to get the value for.
   * @return value, as an Object.
   * @throws IOException
   *     if the field number is greater than or equal to the number of
   *     fields in the tuple.
   */
  public Writable get(int fieldNum) {
    return mFields.get(fieldNum);
  }

  /**
   * Get all of the fields in the tuple as a list.
   *
   * @return List&lt;Object&gt; containing the fields of the tuple in order.
   */
  public List<Writable> getAll() {
    return mFields;
  }

  /**
   * 得到Tuple的数组形式
   *
   * @return 返回Tuple中存放的{@link Writable}数组
   */
  public Writable[] toArray() {
    return mFields.toArray(new Writable[mFields.size()]);
  }

  /**
   * Set the value in a given field.
   *
   * @param fieldNum
   *     Number of the field to set the value for.
   * @param val
   *     Writable to put in the indicated field.
   * @throws IOException
   *     if the field number is greater than or equal to the number of
   *     fields in the tuple.
   */
  public void set(int fieldNum, Writable val) throws IOException {
    mFields.set(fieldNum, val);
  }

  /**
   * 设置Tuple中存放的{@link Writable}对象，复用vals
   *
   * @param vals
   *     待设置的{@link Writable}对象
   */
  public void set(List<Writable> vals) {
    mFields = vals;
  }

  /**
   * 设置Tuple中存放的{@link Writable}对象
   *
   * @param vals
   *     待设置的{@link Writable}对象
   */
  public void set(Writable[] vals) {
    mFields.clear();
    for (Writable val : vals) {
      mFields.add(val);
    }
  }

  /**
   * Append a field to a tuple. This method is not efficient as it may force
   * copying of existing data in order to grow the data structure. Whenever
   * possible you should construct your Tuple with the Tuple(int) method and
   * then fill in the values with set(), rather than construct it with Tuple()
   * and append values.
   *
   * @param val
   *     Object to append to the tuple.
   */
  public void append(Writable val) {
    mFields.add(val);
  }

  /**
   * 将数组中的field添加到Tuple
   *
   * @param vals
   *     待添加的field
   */
  public void append(Writable[] vals) {
    for (Writable val : vals) {
      mFields.add(val);
    }
  }

  /**
   * 将集合中的field添加到Tuple
   *
   * @param vals
   *     待添加的field
   */
  public void append(Collection<Writable> vals) {
    mFields.addAll(vals);
  }

  /**
   * 清空Tuple中的所有field
   */
  public void clear() {
    mFields.clear();
  }

  /**
   * 将Tuple中的所有field使用{@link #DELIM}连接起来，field为null时使用{@link #NULLINDICATOR}代替.
   *
   * @return 连接后的字符串
   */
  public String toDelimitedString() {
    return toDelimitedString(DELIM, NULLINDICATOR);
  }

  /**
   * 将Tuple中的所有field使用指定的delim连接起来，field为null时使用{@link #NULLINDICATOR}代替.
   *
   * @param delim
   *     指定的分割符
   * @return 连接后的字符串
   */
  public String toDelimitedString(char delim) {
    return toDelimitedString(delim, NULLINDICATOR);
  }

  /**
   * 将Tuple中的所有field使用指定的delim连接起来，field为null时使用指定的nullIndicator代替.
   *
   * @param delim
   *     指定的分割符
   * @param nullIndicator
   *     指定的null替代字符串
   * @return 连接后的字符串
   */
  public String toDelimitedString(char delim, String nullIndicator) {
    return CommonUtils.toDelimitedString(
        (Writable[]) mFields.toArray(new Writable[mFields.size()]), delim,
        nullIndicator);
  }

  private String format(Tuple tuple) {
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    for (int i = 0; i < tuple.size(); ++i) {
      Writable d = tuple.get(i);
      if (d != null) {
        if (d instanceof Tuple) {
          Tuple t = (Tuple) d;
          sb.append(format(t));
        } else {
          sb.append(d.toString());
        }
      } else {
        sb.append("");
      }
      if (i != tuple.size() - 1) {
        sb.append(",");
      }
    }
    sb.append(')');
    return sb.toString();
  }

  /**
   * 获取Tuple的字符串表示形式
   */
  @Override
  public String toString() {
    return format(this);
  }

  /**
   * 比较两个Tuple对象，依次比较Tuple中的各个field
   */
  @Override
  public int compareTo(Object other) {
    if (other instanceof Tuple) {
      Tuple t = (Tuple) other;
      int mySz = mFields.size();
      int tSz = t.size();
      if (tSz < mySz) {
        return 1;
      } else if (tSz > mySz) {
        return -1;
      } else {
        for (int i = 0; i < mySz; i++) {
          int c = TupleReaderWriter.compare(mFields.get(i), t.get(i));
          if (c != 0) {
            return c;
          }
        }
        return 0;
      }
    } else {
      return TupleReaderWriter.compare(this, (Writable) other);
    }
  }

  /**
   * 比较两个Tuple对象是否相等
   */
  @Override
  public boolean equals(Object other) {
    return (compareTo(other) == 0);
  }

  /**
   * 获取Tuple对象的hash直
   */
  @Override
  public int hashCode() {
    int hash = 17;
    for (Writable w : mFields) {
      if (w != null) {
        hash = 31 * hash + w.hashCode();
      }
    }

    return hash;
  }

  /**
   * 将Tuple对象序列化至指定的输出流中
   */
  @Override
  public void write(DataOutput out) throws IOException {
    TupleReaderWriter.writeTuple(out, this);
  }

  /**
   * 从指定的输入流中反序列化出Tuple对象
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    // Clear our fields, in case we're being reused.
    mFields.clear();
    TupleReaderWriter.readTuple(in, this);
  }

  /**
   * 生成当前Tuple的拷贝
   */
  @Override
  public Tuple clone() {
    for (Writable field : mFields) {
      if (field instanceof InputBlobWritable
          || field instanceof OutputBlobWritable) {
        throw new RuntimeException(
            "ODPS-0730001: Tuple with BlobWritable not support clone");
      }
    }

    Tuple tuple = new Tuple();
    try {
      ReflectionUtils.cloneWritableInto(tuple, this);
    } catch (IOException ex) {
      throw new RuntimeException(ex.getMessage());
    }
    return tuple;
  }
}
