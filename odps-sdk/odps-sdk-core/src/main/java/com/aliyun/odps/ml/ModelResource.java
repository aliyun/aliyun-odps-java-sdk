package com.aliyun.odps.ml;

/**
 * ModelResource表示在线模型使用的资源
 *
 * @author chao.liu@alibaba-inc.com
 */
public class ModelResource {

  public long getCpu() {
    return cpu;
  }

  public void setCpu(long cpu) {
    this.cpu = cpu;
  }

  public long getMemory() {
    return memory;
  }

  public void setMemory(long memory) {
    this.memory = memory;
  }

  public long getGpu() {
    return gpu;
  }

  public void setGpu(long gpu) {
    this.gpu = gpu;
  }

  private long cpu;
  private long memory;
  private long gpu;
}

