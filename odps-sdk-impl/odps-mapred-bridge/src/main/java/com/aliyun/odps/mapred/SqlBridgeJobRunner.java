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

package com.aliyun.odps.mapred;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mapred.bridge.sqlgen.SqlGenerator;
import com.aliyun.odps.mapred.conf.JobHintTranslator;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.task.JobUtils;
import java.util.HashMap;
import java.util.Map;

public class SqlBridgeJobRunner extends BridgeJobRunner {

  @Override
  protected Instance submitInternal() throws OdpsException {
    Map<String, String> aliases = new HashMap<String, String>();
    aliases.putAll(aliasToTempResource);
    aliases.putAll(SessionState.get().getAliases());
    return JobUtils.submitOpenMR(metaExplorer, SqlGenerator.generate(job, jobId, metaExplorer), taskName,
                                 JobHintTranslator.apply(job), aliases,
                                 job.getInstancePriority());
  }
}
