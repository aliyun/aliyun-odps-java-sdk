package com.aliyun.odps.type;

import java.util.List;

/**
 * Odps struct 接口
 *
 * ODPS 用户请不要自行扩展或实现此接口，否则在未来 ODPS 扩展此接口时会导致错误
 *
 * Created by zhenhong.gzh on 16/7/8.
 */
public interface StructTypeInfo extends NestedTypeInfo {
  List<String> getFieldNames();
  List<TypeInfo> getFieldTypeInfos();
  int getFieldCount();
}
