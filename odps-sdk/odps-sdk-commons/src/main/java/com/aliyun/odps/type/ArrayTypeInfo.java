package com.aliyun.odps.type;

/**
 * Odps array 类型接口
 *
 * ODPS 用户请不要自行扩展或实现此接口，否则在未来 ODPS 扩展此接口时会导致错误
 *
 * Created by zhenhong.gzh on 16/7/8.
 */
public interface ArrayTypeInfo extends TypeInfo {
  TypeInfo getElementTypeInfo();
}
