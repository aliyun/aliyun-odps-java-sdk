package com.aliyun.odps.local.common;

public class Pair<Ty1, Ty2> {

  private Ty1 first;
  private Ty2 second;

  public Pair() {
    this(null, null);
  }

  public Pair(Ty1 first, Ty2 second) {
    this.first = first;
    this.second = second;
  }

  public Ty1 getFirst() {
    return first;
  }

  public void setFirst(Ty1 first) {
    this.first = first;
  }

  public Ty2 getSecond() {
    return second;
  }

  public void setSecond(Ty2 second) {
    this.second = second;
  }

}
