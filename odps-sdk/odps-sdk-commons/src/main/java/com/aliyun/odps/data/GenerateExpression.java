package com.aliyun.odps.data;

/**
 * 生成列表达式
 *
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public interface GenerateExpression {

  String generate(Record record);
}
