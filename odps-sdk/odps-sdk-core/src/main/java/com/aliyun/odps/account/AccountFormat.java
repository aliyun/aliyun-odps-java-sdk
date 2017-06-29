package com.aliyun.odps.account;

/**
 * Created by zhenhong.gzh on 16/12/27.
 */

import java.util.Map;

/**
 * 使用的账号格式
 */
public enum AccountFormat {
  // 用户账号格式为 ID
  ID,

  // 用户账号格式为 displayname
  DISPLAYNAME;

  public static void setParam(AccountFormat accountFormat, Map<String, String> params) {
    if (accountFormat == null || params == null) {
      return;
    }

    params.put("accountformat", accountFormat.name().toLowerCase());
  }
}
