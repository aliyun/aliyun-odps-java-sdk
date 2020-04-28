package com.aliyun.odps.udf.local.util;

import com.aliyun.odps.local.common.Pair;
import com.aliyun.odps.local.common.utils.SchemaUtils;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.udf.annotation.Resolve;
import java.util.List;

public class ResolveUtils {

  public static Pair<List<TypeInfo>, List<TypeInfo>> parseResolve(Resolve resolve) throws IllegalArgumentException{
    if (resolve == null) {
      throw new IllegalArgumentException("You must specify @Resolve annotation.");
    }
    String info = resolve.value()[0];
    return parseResolve(info);
  }

  public static Pair<List<TypeInfo>, List<TypeInfo>> parseResolve(String info) throws IllegalArgumentException {
    String errMsg = "@Resolve({\"" + info + "\"}) ";
    if (info.isEmpty()) {
      throw new IllegalArgumentException(errMsg + "must not be empty string");
    }
    int pos = info.indexOf("->");
    String args = "";
    if (pos > 0) {
      args = info.substring(0, pos);
    } else if (pos < 0) {
      throw new IllegalArgumentException(errMsg);
    }
    int tPos = info.indexOf("->", pos + 2);
    if (tPos >= 0) {
      throw new IllegalArgumentException(errMsg + "contains not exactly one '->'");
    }
    List<TypeInfo> argTypeInfos = SchemaUtils.parseResolveTypeInfo(args);
    if (!validTypeInfo(argTypeInfos)) {
      throw new IllegalArgumentException(errMsg + "annotates wrong arguments '" + args + "'");
    }
    String rtypes = info.substring(pos + 2);
    List<TypeInfo> rtTypeInfos = SchemaUtils.parseResolveTypeInfo(rtypes);
    if (rtTypeInfos.isEmpty()) {
      throw new IllegalArgumentException(errMsg + "annotates no output types '" + args + "'");
    } else if (!validTypeInfo(rtTypeInfos)) {
      throw new IllegalArgumentException(errMsg + "annotates wrong output types '" + rtypes + "'");
    }
    return new Pair(argTypeInfos, rtTypeInfos);
  }

  public static boolean validTypeInfo(List<TypeInfo> typeInfos) {
    if (typeInfos.isEmpty()) {
      return true;
    }
    for (TypeInfo type : typeInfos) {
      String sigType = ArgumentConverterUtils.getSigType(type);
      if (!ArgumentConverterUtils.validSigType.containsKey(sigType)) {
        return false;
      }
    }
    return true;
  }

}
