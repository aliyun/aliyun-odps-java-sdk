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

package com.aliyun.odps.mapred.bridge.sqlgen;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.VolumeInfo;
import com.aliyun.odps.mapred.bridge.MetaExplorer;
import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.conf.SessionState;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.pipeline.Pipeline;


public class SqlGenerator {

  public static String generate(JobConf job, String id, MetaExplorer metaExplorer, Map<String, String> aliasToTempResource) {
    if(!isSqlMode(job, metaExplorer)) {
      return null;
    }
    createFunction((BridgeJobConf) job, id, aliasToTempResource);
    Properties p = new Properties();
    p.setProperty("resource.loader", "class");
    p.setProperty("class.resource.loader.class",
                  "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
    Velocity.init(p);

    VelocityContext context = new VelocityContext();

    Template template = null;

    try {
      String vm = "sql";
      Pipeline pipeline = null;
      if (job.isStreamJob()) {
        vm = "stream";
      } else if (job.isPipeline()) {
        pipeline = Pipeline.fromJobConf(job);
        assert pipeline != null;
        context.put("pipes", pipeline.getNodes());
        vm = "pl";
      }

      context.put("ctx", new SqlGenContext(job, id, pipeline));
      if (!job.getBoolean("odps.optimizer.cbo.remove.shuffle", false)) {
        job.setBoolean("odps.optimizer.cbo.remove.shuffle", false);
      }
      if (!job.getBoolean("odps.optimizer.cbo.enable.dynamic.parallelism", false)) {
        job.setBoolean("odps.optimizer.cbo.enable.dynamic.parallelism", false);
      }
      template = Velocity.getTemplate(String.format("%s.vm", vm));
    } catch (ResourceNotFoundException e) {
      // couldn't find the template
      throw new RuntimeException(e);
    } catch (ParseErrorException e) {
      // syntax error: problem parsing the template
      throw new RuntimeException(e);
    } catch (MethodInvocationException e) {
      // something invoked in the template
      // threw an exception
      throw new RuntimeException(e);
    }

    StringWriter sw = new StringWriter();
    template.merge(context, sw);
    return sw.toString();
  }

  static void createFunction(BridgeJobConf job, String jobId, Map<String, String> aliasToTempResource) {
    StringBuilder resouceText = new StringBuilder();
    StringBuilder createText = new StringBuilder();
    for (String s :job.getFunctionResources()){
      resouceText.append(s).append(",");
    }
    if (job.isStreamJob()) {
      resouceText.deleteCharAt(resouceText.lastIndexOf(","));
      createText.append(String.format("set %s=", SessionState.MR_EXECUTION_SESSION_RESOURCES));
      if (job.get(SessionState.MR_EXECUTION_SESSION_RESOURCES) != null) {
        createText.append(String.format("%s,", job.get(SessionState.MR_EXECUTION_SESSION_RESOURCES)));
      }
      createText.append(String.format("%s;\n", resouceText.toString()));
      job.setFunctionCreateText(createText.toString());
      return;
    }
    for (String s : job.getResources()) {
      if (!aliasToTempResource.keySet().contains(s)) {
        resouceText.append(s).append(",");
      }
    }
    resouceText.deleteCharAt(resouceText.lastIndexOf(","));
    if (job.isPipeline()) {
      int index = 0;
      Pipeline pipeline = Pipeline.fromJobConf(job);
      for (Pipeline.TransformNode node: pipeline.getNodes()) {
        if ("map".equals(node.getType())) {
          createText.append(String.format("create temporary function mr2sql_mapper_%s_%d  as 'com.aliyun.odps.mapred.bridge.LotMapperUDTF' using '%s';\n", jobId, index, resouceText.toString()));
        } else {
          createText.append(String.format("create temporary function mr2sql_reducer_%s_%d  as 'com.aliyun.odps.mapred.bridge.LotReducerUDTF' using '%s';\n", jobId, index, resouceText.toString()));
        }
        index ++ ;
      }
    } else {
      createText.append(String.format("create temporary function mr2sql_mapper_%s as 'com.aliyun.odps.mapred.bridge.LotMapperUDTF' using '%s';\n", jobId, resouceText.toString()));
      if (job.getNumReduceTasks() > 0) {
        createText.append(String.format("create temporary function mr2sql_reducer_%s as 'com.aliyun.odps.mapred.bridge.LotReducerUDTF' using '%s';\n", jobId, resouceText.toString()));
      }
    }
    job.setFunctionCreateText(createText.toString());

    for (String type : Arrays.asList("input", "output")) {
      VolumeInfo[] volumeInfos = type.equals("input") ?  InputUtils.getVolumes(job) : OutputUtils.getVolumes(job);
      if (volumeInfos != null && volumeInfos.length > 0) {
        StringBuilder vols = new StringBuilder();
        for (VolumeInfo vol : volumeInfos) {
          vols.append(vol.toString());
          if (vol.getLabel() != null) {
            vols.append(':').append(vol.getLabel());
          }
          vols.append(",");
        }
        vols.deleteCharAt(vols.lastIndexOf(","));
        job.set(String.format("odps.sql.volume.%s", type), vols.toString());
      }
    }
  }

  /**
   * 返回sql run模式
   *
   * @return true如果符合sql run模式
   */
  static boolean isSqlMode(JobConf job, MetaExplorer metaExplorer) {
    if (System.getProperty(SessionState.MR_EXECUTION_MODE, "lot").equalsIgnoreCase("sql")) {
      return true;
    }
    String mode = job.get(SessionState.MR_EXECUTION_MODE);
    if (mode != null) {
      if (mode.equalsIgnoreCase("lot")) {
        return false;
      } else {
        return true;
      }
    }
    try {
      mode = metaExplorer.getProjectProperty(SessionState.MR_EXECUTION_MODE);
      if (mode != null && (mode.equalsIgnoreCase("sql") || mode.equalsIgnoreCase("hybrid"))) {
        return true;
      }
    } catch (OdpsException e) {
      e.printStackTrace();
      return false;
    }
    return false;
  }
}
