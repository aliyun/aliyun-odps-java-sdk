package com.aliyun.odps.sqa.commandapi;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

class DescribeSchemaCommand extends SQLTaskCommand{

  private static final Map<String, TypeInfo> descSchemaMap = new LinkedHashMap<>();

  static {
    descSchemaMap.put("SchemaName", TypeInfoFactory.STRING);
  }

  @Override
  public List<String> getResultHeaders() {
    return new ArrayList<>(descSchemaMap.keySet());
  }

  @Override
  public List<TypeInfo> getResultTypes() {
    return new ArrayList<>(descSchemaMap.values());
  }

  @Override
  public RecordIter run(Odps odps, CommandInfo commandInfo) throws OdpsException {
    String taskNamePrefix = "desc_schema_";
    return sqlTaskRun(odps, commandInfo, taskNamePrefix);
  }
}
