package com.aliyun.odps;

/**
 * Created by nizheming on 15/8/12.
 */
public class VolumeArchiveResource extends VolumeFileResource {
  public VolumeArchiveResource() {
    super();
    this.model.type = Type.VOLUMEARCHIVE.toString();
  }
}
