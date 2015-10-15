package com.aliyun.odps;

/**
 * Created by nizheming on 15/8/3.
 */
public abstract class VolumeResource extends Resource {

  public VolumeResource() {
    super();
  }

  public String getVolumePath() {
    if (model.volumePath != null && client != null) {
      lazyLoad();
    }

    return model.volumePath;
  }

  public void setVolumePath(String volumePath) {
    model.volumePath = volumePath;
  }
}
