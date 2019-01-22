package com.aliyun.odps.mapred.utils;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class SqlUtils {
  private static Set<Pattern> mr2sqlBlackListPattern = new HashSet<Pattern>();

  static {
    mr2sqlBlackListPattern.add(Pattern.compile("com.aliyun.odps.mapred.bridge.streaming.StreamJob\\s", Pattern.CASE_INSENSITIVE));
  }

  public static boolean mr2SqlCheckPass(String commandText){
    for (Pattern s : mr2sqlBlackListPattern) {
      if (s.matcher(commandText).find()) {
        return  false;
      }
    }
    return true;
  }

}
