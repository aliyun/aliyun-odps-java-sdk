package com.aliyun.odps;

/*
* Interface to produce object
 */
public interface Yieldable<E> {
  void yield(E obj);
  int getTotalYieldCount();
}
