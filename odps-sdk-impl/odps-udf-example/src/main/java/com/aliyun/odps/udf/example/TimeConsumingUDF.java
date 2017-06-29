package com.aliyun.odps.udf.example;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;

/**
 * Example of a time consuming udf simulated by sleeping
 */
public class TimeConsumingUDF extends UDF {

  private ExecutionContext ctx;

  public void setup(ExecutionContext ctx) {
    this.ctx = ctx;
  }

  /**
   * @param secs processing time in seconds
   * @return "OK"
   */
  public String evaluate(Long secs) throws UDFException {
    // long-processing-time computation simulated by sleeping
    for (long i = 0; i < secs; i++) {
      try {
        Thread.sleep(1000);
        // call claimAlive to avoid being killed by timeout detector
        ctx.claimAlive();
      } catch (InterruptedException e) {
        throw new UDFException(e);
      }
    }
    return "OK";
  }
}
