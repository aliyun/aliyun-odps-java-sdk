package com.aliyun.odps.type;

import com.aliyun.odps.OdpsType;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public interface NestedTypeInfo extends TypeInfo {
  /**
   * �˷����Ὣstruct�����У����ֶν���ת�塣������ƴװ SQL ����
   * �� quote = false ʱ���˷����� getTypeName() ��ͬ
   */
  String getTypeName(boolean quote);
}
