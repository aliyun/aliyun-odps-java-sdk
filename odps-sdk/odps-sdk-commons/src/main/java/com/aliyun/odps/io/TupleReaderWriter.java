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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.utils.ReflectionUtils;

/**
 * TupleReaderWriter 提供 {@link Tuple} 的序列化和反序列化方法.
 */
public class TupleReaderWriter {

  // IMPORTANT! This list can be used to record values of data on disk,
  // so do not change the values. You may strand user data.
  // IMPORTANT! Order matters here, as compare() below uses the order to
  // order unlike datatypes. Don't change this ordering.
  // Spaced unevenly to leave room for new entries without changing
  // values or creating order issues.
  private static final byte UNKNOWN = 0;
  private static final byte NULL = 1;
  private static final byte NULLWRITABLE = 2;
  private static final byte BOOLEANWRITABLE = 3;
  private static final byte BYTESWRITABLE = 4;
  private static final byte INTWRITABLE = 5;
  private static final byte LONGWRITABLE = 6;
  private static final byte DATETIMEWRITABLE = 7;
  private static final byte DOUBLEWRITABLE = 8;
  private static final byte TEXT = 9;

  private static final byte TUPLE = 100;

  private static Log LOG = LogFactory.getLog(TupleReaderWriter.class);

  private static byte findType(Writable o) {
    if (o == null) {
      return NULL;
    }

    // Try to put the most common first
    if (o instanceof LongWritable) {
      return LONGWRITABLE;
    } else if (o instanceof IntWritable) {
      return INTWRITABLE;
    } else if (o instanceof Text) {
      return TEXT;
    } else if (o instanceof DoubleWritable) {
      return DOUBLEWRITABLE;
    } else if (o instanceof BooleanWritable) {
      return BOOLEANWRITABLE;
    } else if (o instanceof DatetimeWritable) {
      return DATETIMEWRITABLE;
    } else if (o instanceof BytesWritable) {
      return BYTESWRITABLE;
    } else if (o instanceof NullWritable) {
      return NULLWRITABLE;
    } else if (o instanceof Tuple) {
      return TUPLE;
    }

    return UNKNOWN;
  }

  /**
   * /** Compare two objects to each other. This function is necessary because
   * there's no super class that implements compareTo. This function provides an
   * (arbitrary) ordering of objects of different types as follows: NULL &lt;
   * BOOLEAN &lt; BYTE &lt; INTEGER &lt; LONG &lt; FLOAT &lt; DOUBLE * &lt;
   * BYTEARRAY &lt; STRING &lt; MAP &lt; TUPLE &lt; BAG. No other functions
   * should implement this cross object logic. They should call this function
   * for it instead.
   *
   * @param o1
   *     First object
   * @param o2
   *     Second object
   * @return -1 if o1 is less, 0 if they are equal, 1 if o2 is less.
   */
  public static int compare(Writable o1, Writable o2) {

    byte dt1 = findType(o1);
    byte dt2 = findType(o2);
    return compare(o1, o2, dt1, dt2);
  }

  /**
   * Same as {@link #compare(Object, Object)}, but does not use reflection to
   * determine the type of passed in objects, relying instead on the caller to
   * provide the appropriate values, as determined by {@link findType(Object)}.
   *
   * Use this version in cases where multiple objects of the same type have to
   * be repeatedly compared.
   *
   * @param o1
   *     first object
   * @param o2
   *     second object
   * @param dt1
   *     type, as byte value, of o1
   * @param dt2
   *     type, as byte value, of o2
   * @return -1 if o1 is &lt; o2, 0 if they are equal, 1 if o1 &gt; o2
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static int compare(Writable o1, Writable o2, byte dt1, byte dt2) {
    if (dt1 == dt2) {
      switch (dt1) {
        case NULL:
        case NULLWRITABLE:
          return 0;

        case BOOLEANWRITABLE:
          return ((BooleanWritable) o1).compareTo((BooleanWritable) o2);

        case BYTESWRITABLE:
          return ((BytesWritable) o1).compareTo((BytesWritable) o2);

        case INTWRITABLE:
          return ((IntWritable) o1).compareTo((IntWritable) o2);

        case LONGWRITABLE:
          return ((LongWritable) o1).compareTo((LongWritable) o2);

        case DATETIMEWRITABLE:
          return ((DatetimeWritable) o1).compareTo((DatetimeWritable) o2);

        case DOUBLEWRITABLE:
          return ((DoubleWritable) o1).compareTo((DoubleWritable) o2);

        case TEXT:
          return ((Text) o1).compareTo((Text) o2);

        case TUPLE:
          return ((Tuple) o1).compareTo((Tuple) o2);

        case UNKNOWN:
          if (o1 instanceof WritableComparable
              && o2 instanceof WritableComparable) {
            return ((WritableComparable) o1).compareTo((WritableComparable) o2);
          }
          throw new RuntimeException("ODPS-0730001: Class "
                                     + o1.getClass().getName() + " is not comparable");

        default:
          throw new RuntimeException("Not support type " + dt1 + " in compare");
      }
    } else if (dt1 < dt2) {
      return -1;
    } else {
      return 1;
    }
  }

  /**
   * {@link Tuple} 对象的 {@link WritableComparator} 自然顺序实现（升序）.
   */
  public static class TupleRawComparator extends WritableComparator {

    public TupleRawComparator() {
      super(Tuple.class, true);
    }

  }

  /**
   * 从指定的输入流反序列化至指定的Tuple对象
   *
   * @param in
   *     输入流，含有Tuple的field的字节
   * @param t
   *     反序列化后的Tuple对象
   * @throws IOException
   *     输入流中的字节不是某个Tuple序列化后的字节
   */
  public static void readTuple(DataInput in, Tuple t) throws IOException {
    // Make sure it's a tuple.
    byte b = in.readByte();
    if (b != TUPLE) {
      String msg = "Unexpected data while reading tuple from binary file.";
      throw new IOException(msg);
    }

    // Read the number of fields
    int sz = in.readInt();
    for (int i = 0; i < sz; i++) {
      byte type = in.readByte();
      t.append(readDatum(in, type));
    }
  }

  @SuppressWarnings("unchecked")
  private static Writable readDatum(DataInput in, byte type) throws IOException {
    switch (type) {
      case TUPLE:
        int sz = in.readInt();
        // if sz == 0, we construct an "empty" tuple -
        // presumably the writer wrote an empty tuple!
        if (sz < 0) {
          throw new IOException("Invalid size " + sz + " for a tuple");
        }
        Tuple tp = new Tuple(sz);
        for (int i = 0; i < sz; i++) {
          byte b = in.readByte();
          tp.set(i, readDatum(in, b));
        }

        return tp;

      case NULL:
        return null;

      case INTWRITABLE:
        IntWritable iw = new IntWritable();
        iw.readFields(in);
        return iw;

      case LONGWRITABLE:
        LongWritable lw = new LongWritable();
        lw.readFields(in);
        return lw;

      case DATETIMEWRITABLE:
        DatetimeWritable dtw = new DatetimeWritable();
        dtw.readFields(in);
        return dtw;

      case DOUBLEWRITABLE:
        DoubleWritable dw = new DoubleWritable();
        dw.readFields(in);
        return dw;

      case BOOLEANWRITABLE:
        BooleanWritable bw = new BooleanWritable();
        bw.readFields(in);
        return bw;

      case BYTESWRITABLE:
        BytesWritable bsw = new BytesWritable();
        bsw.readFields(in);
        return bsw;

      case TEXT:
        Text t = new Text();
        t.readFields(in);
        return t;

      case NULLWRITABLE:
        NullWritable nw = NullWritable.get();
        nw.readFields(in);
        return nw;

      case UNKNOWN:
        String clsName = in.readUTF();
        try {
          Class<? extends Writable> cls = (Class<? extends Writable>) Class
              .forName(clsName);
          Writable w = (Writable) ReflectionUtils.newInstance(cls, null);
          w.readFields(in);
          return w;
        } catch (RuntimeException re) {
          LOG.info(re.getMessage());
          throw new IOException(re);
        } catch (ClassNotFoundException cnfe) {
          throw new IOException(cnfe);
        }

      default:
        throw new RuntimeException("Unexpected data type " + type
                                   + " found in stream.");
    }
  }

  /**
   * 将指定的Tuple对象序列化至指定的输出流中
   *
   * @param out
   *     Tuple对象要写入的输出流
   * @param t
   *     待写出的Tuple对象
   * @throws IOException
   *     待序列化的Tuple对象在序列化其field对象出现异常
   */
  public static void writeTuple(DataOutput out, Tuple t) throws IOException {
    out.writeByte(TUPLE);
    int sz = t.size();
    out.writeInt(sz);
    for (int i = 0; i < sz; i++) {
      writeDatum(out, t.get(i));
    }
  }

  private static void writeDatum(DataOutput out, Writable val)
      throws IOException {
    // Read the data type
    byte type = findType(val);
    switch (type) {
      case TUPLE:
        Tuple t = (Tuple) val;
        out.writeByte(TUPLE);
        int sz = t.size();
        out.writeInt(sz);
        for (int i = 0; i < sz; i++) {
          writeDatum(out, t.get(i));
        }
        break;

      case NULL:
        out.writeByte(NULL);
        break;

      case INTWRITABLE:
        out.writeByte(INTWRITABLE);
        ((IntWritable) val).write(out);
        break;

      case LONGWRITABLE:
        out.writeByte(LONGWRITABLE);
        ((LongWritable) val).write(out);
        break;

      case DATETIMEWRITABLE:
        out.writeByte(DATETIMEWRITABLE);
        ((DatetimeWritable) val).write(out);
        break;

      case DOUBLEWRITABLE:
        out.writeByte(DOUBLEWRITABLE);
        ((DoubleWritable) val).write(out);
        break;

      case BOOLEANWRITABLE:
        out.writeByte(BOOLEANWRITABLE);
        ((BooleanWritable) val).write(out);
        break;

      case BYTESWRITABLE:
        out.writeByte(BYTESWRITABLE);
        ((BytesWritable) val).write(out);
        break;

      case TEXT:
        out.writeByte(TEXT);
        ((Text) val).write(out);
        break;

      case NULLWRITABLE:
        out.writeByte(NULLWRITABLE);
        ((NullWritable) val).write(out);
        break;

      case UNKNOWN:
        out.writeByte(UNKNOWN);
        out.writeUTF(val.getClass().getName());
        val.write(out);
        break;

      default:
        throw new RuntimeException("Unexpected data type " + type
                                   + " found in stream.");
    }
  }
}
