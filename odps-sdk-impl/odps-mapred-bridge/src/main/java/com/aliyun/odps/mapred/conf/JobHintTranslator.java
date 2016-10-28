/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.mapred.conf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class JobHintTranslator {
  private static HashSet<String> reservedKeys = new HashSet<String>();
  static {
    reservedKeys.add("odps.mapred.mapoutput.key.schema");
    reservedKeys.add("odps.mapred.mapoutput.value.schema");
    reservedKeys.add("odps.mapred.map.class");
    reservedKeys.add("odps.mapred.reduce.class");
    reservedKeys.add("odps.mapred.combine.class");
    reservedKeys.add("odps.mapred.partition.class");
  }
  static int MAX_VALUE_LENGTH = 10240; // 10k limit

  public static Map<String, String> apply(JobConf job) {
    Map<String, String> hint = new HashMap<String, String>();

    Iterator<Entry<String, String>> itr = job.iterator();
    while (itr.hasNext()) {
      Entry<String, String> e = itr.next();
      if (e.getKey().startsWith("odps.mapred") &&
          !reservedKeys.contains(e.getKey()) && e.getValue().length() > MAX_VALUE_LENGTH) {
        // Skipping all mapred related parameters not reserved or too big value especially so as to workaround with the
        //  `source xml key/value 128k limit' issue.
        continue;
      }
      hint.put(e.getKey(), e.getValue());
    }

    return hint;
  }
}
