/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aliyun.odps.volume;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.VolumeException;
import com.aliyun.odps.tunnel.VolumeFSErrorCode;

/**
 * A proxy that handle all common problems caused by volume path
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public abstract class VolumeFSJobRunnerProxy<T> {

  public static final String CUR_EXCEPTION = "volume_job_cur_exception";

  /**
   * Do main job
   * 
   * @param path
   * @param params
   * @throws VolumeException
   */
  abstract public T doJob(String path, Map<String, Object> params) throws VolumeException;

  /**
   * Handle {@link VolumeException} which contains errCode "VolumeMissing"
   * 
   * @param path
   * @param params
   * @throws VolumeException
   */
  abstract public T onVolumeMissing(String path, Map<String, Object> params) throws VolumeException;


  /**
   * Handle {@link VolumeException} which contains errCode "NoSuchMissing"
   * 
   * @param path
   * @param params
   * @throws VolumeException
   */
  abstract public T onNoSuchVolume(String path, Map<String, Object> params) throws VolumeException;


  /**
   * Handle {@link VolumeException} which contains errCode "InvalidPath"
   * 
   * @param path
   * @param params
   * @throws VolumeException
   */
  public T onInvalidPath(String path, Map<String, Object> params) throws VolumeException {
    VolumeException e = (VolumeException) params.get(CUR_EXCEPTION);
    throw e;
  }

  /**
   * Handle {@link VolumeException} which contains errCode "PathAlreadyExists"
   * 
   * @param path
   * @param params
   * @throws VolumeException
   */
  public T onPathAlreadyExists(String path, Map<String, Object> params) throws VolumeException {
    VolumeException e = (VolumeException) params.get(CUR_EXCEPTION);
    throw e;
  }

  /**
   * Run job proxy
   * 
   * @param path
   * @param params
   * @throws VolumeException
   */
  public T run(String path, Map<String, Object> params) throws VolumeException {
    try {
      VolumeFSUtil.checkPath(path);
      return doJob(path, params);
    } catch (VolumeException e) {
      if (params == null) {
        params = new HashMap<String, Object>();
      }
      params.put(CUR_EXCEPTION, e);
      if (VolumeFSErrorCode.NoSuchVolume.equalsIgnoreCase(e.getErrCode())) {
        return onNoSuchVolume(path, params);
      } else if (VolumeFSErrorCode.VolumeMissing.equalsIgnoreCase(e.getErrCode())) {
        return onVolumeMissing(path, params);
      } else if (VolumeFSErrorCode.InvalidPath.equalsIgnoreCase(e.getErrCode())) {
        return onInvalidPath(path, params);
      } else if (VolumeFSErrorCode.PathAlreadyExists.equalsIgnoreCase(e.getErrCode())) {
        return onPathAlreadyExists(path, params);
      }
      throw e;
    }
  }


}
