package com.aliyun.odps.udf;

/*
 * Interface for collecting data
 */
public interface DataCollector
{
  public void collect(Object[] args);
}
