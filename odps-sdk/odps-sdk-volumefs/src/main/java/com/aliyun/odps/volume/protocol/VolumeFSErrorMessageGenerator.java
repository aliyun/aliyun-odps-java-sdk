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
package com.aliyun.odps.volume.protocol;


/**
 * Standard VolumeFS Error Message Generator
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFSErrorMessageGenerator {

  public static String noSuchFileOrDirectory(String path) {
    return "'" + path + "': No such file or directory";
  }

  public static String fileExists(String path) {
    return "'" + path + "': File exists";
  }

  public static String isADirectory(String path) {
    return "'" + path + "': Is a directory";
  }

  public static String isNotAValidODPSVolumeFSFilename(String path) {
    return "Pathname " + path + " is not a valid ODPSVolumeFS filename";
  }

  public static String theOpreationIsNotAllowed(String msg) {
    return "The opreation is not allowed:" + msg;
  }

  public static String oldVolumeAlert(String volumeName) {
    return "The Volume：" + volumeName
        + " you accessed is an old volume which dose not support volumefs feature!";
  }

}
