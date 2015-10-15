package com.aliyun.odps;

/**
 * Created by nizheming on 15/8/3.
 */
public class VolumeFileResource extends VolumeResource {
  public VolumeFileResource() {
    super();
    this.model.type = Type.VOLUMEFILE.toString();
  }
}
