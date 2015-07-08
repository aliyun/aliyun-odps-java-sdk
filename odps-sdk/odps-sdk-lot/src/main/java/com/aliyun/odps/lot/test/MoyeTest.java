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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.lot.Task;
import com.aliyun.odps.lot.common.IllegalOperationException;
import com.aliyun.odps.lot.common.Language;
import com.aliyun.odps.lot.common.Reference;
import com.aliyun.odps.lot.common.ReferencedURI;
import com.aliyun.odps.lot.common.Resource;
import com.aliyun.odps.lot.common.ScalarExpression;
import com.aliyun.odps.lot.common.ScalarFunction;
import com.aliyun.odps.lot.common.Schema;
import com.aliyun.odps.lot.common.TableValuedFunction;
import com.aliyun.odps.lot.operators.Apply;
import com.aliyun.odps.lot.operators.DistributeBy;
import com.aliyun.odps.lot.operators.Join;
import com.aliyun.odps.lot.operators.LanguageSink;
import com.aliyun.odps.lot.operators.LanguageSource;
import com.aliyun.odps.lot.operators.Select;
import com.aliyun.odps.lot.operators.Select.Expression;

public class MoyeTest {

  public static void main(String[] args) throws IllegalOperationException, OdpsException {
    if (args.length != 1) {
      System.out
          .println("Usage: java -classpath xxxx com.aliyun.odps.lot.test.MoyeTest output_file");
      System.exit(-1);
    }

    Odps odps = OdpsTestUtils.newDefaultOdps();
    String name = "yongfeng.chai_01234567890123456789";
    String project = "chai";
    String tblScanClass = "read_table_data_func";
    ArrayList<Resource> resList = new ArrayList<Resource>();

    String udtfName = "py_split";

    Task task = new Task(odps, name);
    task.addHint("DefaultProject", project);
    task.addHint("odps.lot.optimizer.disable.customed.shuffle", "true");

    // LanguageSource_1
    ArrayList<ReferencedURI> ls_1_uris = new ArrayList<ReferencedURI>();
    Schema ls_1_schema = new Schema();
    ls_1_schema.addColumn(OdpsType.STRING, "val");
    int ls_1_ins_count = 15;
    HashMap<String, String> ls_1_props = new HashMap<String, String>();
    ls_1_props.put("ls_1.partition.ds", "20141101");
    LanguageSource ls_1 = new LanguageSource(Language.Java, resList, tblScanClass,
                                             ls_1_uris, ls_1_schema, ls_1_ins_count, ls_1_props);
    ls_1.setId("MOYE_LanguageSource_1");

    // Apply_2
    // col.map(word => (word, 1))
    ArrayList<ScalarExpression> app_2_params = new ArrayList<ScalarExpression>();
    app_2_params.add(new Reference("val", ls_1));
    ArrayList<String> app_2_output = new ArrayList<String>();
    app_2_output.add("val");
    TableValuedFunction
        app_2_func =
        new TableValuedFunction(udtfName, project, app_2_params, app_2_output);
    ArrayList<TableValuedFunction> app_2_funcs = new ArrayList<TableValuedFunction>();
    app_2_func.addProperties("app_2.mcode", "mcode-app_2");
    app_2_func.addProperties("app_2.wcode", "wcode-app_2");
    app_2_funcs.add(app_2_func);
    Apply app_2 = new Apply(app_2_funcs);
    app_2.setId("MOYE_Apply_2");
    ls_1.addChild(app_2);

    // Shuffle_3
    ArrayList<Reference> shuffle_3_cols = new ArrayList<Reference>();
    shuffle_3_cols.add(new Reference("val", ls_1));
    DistributeBy shuffle_3 = new DistributeBy(shuffle_3_cols);
    shuffle_3.setId("MOYE_Shuffle_3");
    app_2.addChild(shuffle_3);

    // Apply_4
    ArrayList<ScalarExpression> app_4_params = new ArrayList<ScalarExpression>();
    app_4_params.add(new Reference("val", ls_1));
    ArrayList<String> app_4_output = new ArrayList<String>();
    app_4_output.add("val");
    TableValuedFunction
        app_4_func =
        new TableValuedFunction(udtfName, project, app_4_params, app_4_output);
    app_4_func.addProperties("app_4.mcode", "mcode-app_4");
    app_4_func.addProperties("app_4.wcode", "wcode-app_4");
    ArrayList<TableValuedFunction> app_4_funcs = new ArrayList<TableValuedFunction>();
    app_4_funcs.add(app_4_func);
    Apply app_4 = new Apply(app_4_funcs);
    app_4.setId("MOYE_Apply_4");
    shuffle_3.addChild(app_4);

    // Select_5
    ArrayList<Expression> sel_5_exprs = new ArrayList<Expression>();
    sel_5_exprs.add(new Select.Expression("val", new Reference("val", ls_1)));
    Select sel_5 = new Select(sel_5_exprs);
    sel_5.setId("MOYE_Select_5");
    app_4.addChild(sel_5);

    // Apply_6
    ArrayList<ScalarExpression> app_6_params = new ArrayList<ScalarExpression>();
    app_6_params.add(new Reference("val", ls_1));
    ArrayList<String> app_6_output = new ArrayList<String>();
    app_6_output.add("val");
    TableValuedFunction
        app_6_func =
        new TableValuedFunction(udtfName, project, app_6_params, app_6_output);
    app_6_func.addProperties("app_6.mcode", "mcode-app_6");
    app_6_func.addProperties("app_6.wcode", "wcode-app_6");
    ArrayList<TableValuedFunction> app_6_funcs = new ArrayList<TableValuedFunction>();
    app_6_funcs.add(app_6_func);
    Apply app_6 = new Apply(app_6_funcs);
    app_6.setId("MOYE_Apply_6");
    app_4.addChild(app_6);

    // Shuffle_7
    ArrayList<Reference> shuffle_7_cols = new ArrayList<Reference>();
    shuffle_7_cols.add(new Reference("val", ls_1));
    DistributeBy shuffle_7 = new DistributeBy(shuffle_7_cols);
    shuffle_7.setId("MOYE_Shuffle_7");
    app_6.addChild(shuffle_7);

    // Join_8
    ArrayList<ScalarExpression> join_8_params = new ArrayList<ScalarExpression>();
    join_8_params.add(new Reference("val", ls_1));
    join_8_params.add(new Reference("val", sel_5));
    ScalarExpression join_8_condition = new ScalarFunction(project, "EQ", join_8_params);
    Join join_8 = new Join(sel_5, shuffle_7, Join.Type.Inner, join_8_condition);
    join_8.setExecutionHint(Join.ExecutionHint.ShuffledHashJoin);
    join_8.setId("MOYE_Join_8");
    sel_5.addChild(join_8);
    shuffle_7.addChild(join_8);

    // LanguageSink_9
    ArrayList<ReferencedURI> ls_9_uris = new ArrayList<ReferencedURI>();
    ArrayList<LanguageSink.Output> ls_9_schema = new ArrayList<LanguageSink.Output>();
    ls_9_schema.add(new LanguageSink.Output(new Reference("val", ls_1), OdpsType.STRING));
    int ls_9_ins_count = 3;
    HashMap<String, String> ls_9_props = new HashMap<String, String>();
    ls_9_props.put("ls_9.partition.ds", "20141103");
    LanguageSink ls_9 = new LanguageSink(Language.Java, resList, tblScanClass,
                                         ls_9_uris, ls_9_schema, ls_9_ins_count, ls_9_props);
    ls_9.setId("MOYE_LanguageSource_9");
    join_8.addChild(ls_9);

    task.addRootOperator(ls_1);

    Instance ins = task.run();
    ins.waitForSuccess();
    Map<String, TaskStatus> states = ins.getTaskStatus();
    TaskStatus state = states.get(name);
    String dateil = ins.getTaskDetailJson(name);
    System.out.println("State: " + state.getName());
    System.out.println("Detail: " + dateil);
  }
}
