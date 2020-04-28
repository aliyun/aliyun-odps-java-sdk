package com.aliyun.odps.udf.local.examples;

import com.aliyun.odps.io.ArrayWritable;
import com.aliyun.odps.io.IntWritable;
import com.aliyun.odps.io.MapWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.annotation.Resolve;

@Resolve("array<string>->map<string, int>")
public class UdafComplex extends Aggregator {

  @Override
  public Writable newBuffer() {
    return new MapWritable();
  }

  @Override
  public void iterate(Writable buffer, Writable[] args) {
    MapWritable result = (MapWritable) buffer;
    ArrayWritable arg = (ArrayWritable) args[0];
    if (arg != null) {
      Writable[] values = arg.get();
      if (values != null && values.length > 0) {
        for (Writable value : values) {
          Text text = (Text) value;
          IntWritable count = (IntWritable) result.get(text);
          if (count == null) {
            count = new IntWritable(1);
            result.put(text, count);
          } else {
            count.set(count.get() + 1);
          }
        }
      }
    }
  }

  @Override
  public void merge(Writable buffer, Writable partial) {
    MapWritable result = (MapWritable) buffer;
    MapWritable partialResult = (MapWritable) partial;
    partialResult.forEach((k,v)->{
      IntWritable count = (IntWritable) result.get(k);
      if (count == null) {
        result.put(k, v);
      } else {
        IntWritable partCount = (IntWritable) v;
        count.set(count.get() + partCount.get());
      }
    });
  }

  @Override
  public Writable terminate(Writable buffer) {
    return buffer;
  }

}
