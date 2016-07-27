package com.aliyun.odps.udf.example;

import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDF;

/**
 *
 */
public class Counters extends UDF {

  enum CounterGroup {
    WORD_COUNT
  }

  private ExecutionContext ctx;

  @Override
  public void setup(ExecutionContext ctx) {
    this.ctx = ctx;
  }

  public String evaluate(String word) {
    Counter counter = ctx.getCounter(CounterGroup.WORD_COUNT);
    for (String token: word.split("\\s+")) {
      counter.increment(1);
    }
    return word;
  }

}
