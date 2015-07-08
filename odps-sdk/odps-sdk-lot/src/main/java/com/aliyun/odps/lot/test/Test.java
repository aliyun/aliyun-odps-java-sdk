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

package com.aliyun.odps.lot.test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.lot.Task;
import com.aliyun.odps.lot.common.AggregationFunction;
import com.aliyun.odps.lot.common.Constant;
import com.aliyun.odps.lot.common.IllegalOperationException;
import com.aliyun.odps.lot.common.Language;
import com.aliyun.odps.lot.common.PartitionSpecification;
import com.aliyun.odps.lot.common.Reference;
import com.aliyun.odps.lot.common.ReferencedURI;
import com.aliyun.odps.lot.common.Resource;
import com.aliyun.odps.lot.common.ScalarExpression;
import com.aliyun.odps.lot.common.ScalarFunction;
import com.aliyun.odps.lot.common.Schema;
import com.aliyun.odps.lot.operators.Aggregate;
import com.aliyun.odps.lot.operators.Filter;
import com.aliyun.odps.lot.operators.Join;
import com.aliyun.odps.lot.operators.Join.ExecutionHint;
import com.aliyun.odps.lot.operators.Join.Type;
import com.aliyun.odps.lot.operators.LanguageSource;
import com.aliyun.odps.lot.operators.Select;
import com.aliyun.odps.lot.operators.Select.Expression;
import com.aliyun.odps.lot.operators.TableScan;
import com.aliyun.odps.lot.operators.TableSink;

public class Test {

  public static void main(String[] args) throws IllegalOperationException, OdpsException {
    if (args.length != 1) {
            /* /usr/ali/bin/java $trunk/sdk/java/releases/odps-sdk-core/odps-sdk-core-0.14.0-SNAPSHOT.jar:$trunk/odps_planner/relational/java/build/dist/odps_planner_protocol.jar:$trunk/sdk/java/odps-sdk/odps-sdk-lot/target/classes/ com.aliyun.odps.lot.test.Test /tmp/aabb */
      System.out.println("Usage: java -classpath xxxx com.aliyun.odps.lot.test.Test output_file");
      System.exit(-1);
    }

    String accessId = "your access id";
    String accessKey = "your access key";
    Account account = new AliyunAccount(accessId, accessKey);

    Odps odps = new Odps(account);
    String name = "yongfeng.chai";
    String project = "default_prj";

    Task task = new Task(odps, name);
    task.addHint("DefaultProject", project);

    // TableScan_1
    TableScan ts_1 = new TableScan(project, "table_src_1");

    // Filter_2    ds = 'partition_1'
    List<ScalarExpression> fil_2_params = new ArrayList<ScalarExpression>();
    fil_2_params.add(new Reference("ds", ts_1));
    Constant fil_2_const = new Constant();
    fil_2_const.setStringValue("src_partition_1");
    fil_2_params.add(fil_2_const);
    // TODO 这里应该有一个builtin func的列表，要不然用户不知道怎么填func name
    ScalarFunction fil_2_condition = new ScalarFunction(project, "EQUAL", fil_2_params);
    Filter fil_2 = new Filter(fil_2_condition);
    ts_1.addChild(fil_2);

    // LanguageSource_3
    ArrayList<Resource> ls_3_res = new ArrayList<Resource>();
    ls_3_res.add(new Resource(project, "aabbccdd.jar"));
    String ls_3_class_name = "main_func";
    ArrayList<ReferencedURI> ls_3_uris = new ArrayList<ReferencedURI>();
    Schema ls_3_schema = new Schema();
    ls_3_schema.addColumn(OdpsType.BIGINT, "key");
    ls_3_schema.addColumn(OdpsType.DOUBLE, "val");
    ls_3_schema.addColumn(OdpsType.STRING, "ds");
    int ls_3_ins_count = 5;
    HashMap<String, String> ls_3_props = new HashMap<String, String>();
    LanguageSource ls_3 = new LanguageSource(Language.Java, ls_3_res, ls_3_class_name,
                                             ls_3_uris, ls_3_schema, ls_3_ins_count, ls_3_props);

    // Filter_4     ds = 'partition_2'
    List<ScalarExpression> fil_4_params_1 = new ArrayList<ScalarExpression>();
    fil_4_params_1.add(new Reference("ds", ls_3));
    Constant fil_4_const_1 = new Constant();
    fil_4_const_1.setStringValue("src_partition_2");
    fil_4_params_1.add(fil_4_const_1);
    ScalarFunction fil_4_expr_1 = new ScalarFunction(project, "EQUAL", fil_4_params_1);

    List<ScalarExpression> fil_4_params_2 = new ArrayList<ScalarExpression>();
    fil_4_params_2.add(new Reference("ds", ls_3));
    Constant fil_4_const_2 = new Constant();
    fil_4_const_2.setStringValue("src_partition_3");
    fil_4_params_2.add(fil_4_const_2);
    ScalarFunction fil_4_expr_2 = new ScalarFunction(project, "EQUAL", fil_4_params_2);

    List<ScalarExpression> fil_4_params_3 = new ArrayList<ScalarExpression>();
    fil_4_params_3.add(fil_4_expr_1);
    fil_4_params_3.add(fil_4_expr_2);
    ScalarFunction fil_4_condition = new ScalarFunction(project, "OR", fil_4_params_3);
    Filter fil_4 = new Filter(fil_4_condition);
    ls_3.addChild(fil_4);

    // Join_5   table_src_1.key = table_src_2.key
    List<ScalarExpression> join_5_params = new ArrayList<ScalarExpression>();
    join_5_params.add(new Reference("key", ts_1));
    join_5_params.add(new Reference("key", ls_3));
    ScalarFunction join_5_condition = new ScalarFunction(project, "EQUAL", join_5_params);

    Join join_5 = new Join(fil_2, fil_4, Type.Inner, join_5_condition);
    join_5.setExecutionHint(ExecutionHint.FullHashJoin);
    List<String> smallParents = new ArrayList<String>();
    smallParents.add(fil_2.getId());
    join_5.setSmallParents(smallParents);
    fil_2.addChild(join_5);
    fil_4.addChild(join_5);

    // Filter_6     table_src_2.key > 0
    List<ScalarExpression> fil_6_params = new ArrayList<ScalarExpression>();
    fil_6_params.add(new Reference("key", ls_3));
    Constant fil_6_const = new Constant();
    fil_6_const.setIntValue(0);
    fil_6_params.add(fil_6_const);
    ScalarFunction fil_6_condition = new ScalarFunction(project, "GT", fil_6_params);

    Filter fil_6 = new Filter(fil_6_condition);
    join_5.addChild(fil_6);

    // Aggregate_7      count(distinct key)
    List<ScalarExpression> aggr_7_params = new ArrayList<ScalarExpression>();
    aggr_7_params.add(new Reference("key", ls_3));
    AggregationFunction aggrFunc = new AggregationFunction(project, "COUNT", true, aggr_7_params);

    Map<String, AggregationFunction> aggrFuncMap = new HashMap<String, AggregationFunction>();
    aggrFuncMap.put("cdk", aggrFunc);

    List<Reference> gbyCols = new ArrayList<Reference>();
    gbyCols.add(new Reference("key", ls_3));

    Aggregate aggr_7 = new Aggregate(gbyCols, aggrFuncMap);
    fil_6.addChild(aggr_7);

    // Select_8     select cdk
    List<Expression> exprList = new ArrayList<Expression>();
    Expression expr = new Expression("sel_cdk", new Reference("cdk", aggr_7));
    exprList.add(expr);
    Select sel_8 = new Select(exprList);
    aggr_7.addChild(sel_8);

    // TableWrite_9
    Map<String, Constant> definition = new HashMap<String, Constant>();
    Constant tw_9_const_1 = new Constant();
    tw_9_const_1.setStringValue("dest_partition_4");
    definition.put("pt", tw_9_const_1);
    Constant tw_9_const_2 = new Constant();
    tw_9_const_2.setStringValue("20141030");
    definition.put("ds", tw_9_const_2);
    PartitionSpecification part = new PartitionSpecification(definition);
    TableSink tw_9 = new TableSink(project, "table_dest_1", true, part);
    sel_8.addChild(tw_9);

    task.addRootOperator(ts_1);
    task.addRootOperator(ls_3);

    ByteArrayInputStream stream = task.getTaskInputStream();

    try {
      File file = new File(args[0]);
      if (file.isFile() && file.exists()) {
        file.delete();
      }

      BufferedOutputStream bufOutputStream = new BufferedOutputStream(new FileOutputStream(file));
      byte[] tmp = new byte[1];
      while (stream.read(tmp) != -1) {
        bufOutputStream.write(tmp);
      }
      stream.close();
      bufOutputStream.flush();
      bufOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
