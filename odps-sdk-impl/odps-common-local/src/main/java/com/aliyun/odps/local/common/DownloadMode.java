package com.aliyun.odps.local.common;

/**
 * 
 * Local模式下载数据的行为，分以下三种模式： <br/>
 * （1）NEVER: 永远都不会从服务器端下载任何数据 <br/>
 * （2）AUTO: 当本地warehouse中不存在表或资源时，才会从服务器端下载 <br/>
 * （3）ALWAYS: 即使本地wahouse中存在表或资源，也会从服务器端下载，如果服务器端不存在，会抛出响应错误<br/>
 * 通过参数：odps.mapred.local.download.mode控制
 *
 */
public enum DownloadMode {
  NEVER, AUTO, ALWAYS
}
