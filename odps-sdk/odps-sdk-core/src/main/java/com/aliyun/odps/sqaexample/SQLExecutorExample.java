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

package com.aliyun.odps.sqaexample;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.sqa.*;

import java.io.IOException;
import java.util.*;

public class SQLExecutorExample {

    public static void SimpleExample() {
        // 设置账号和project信息
        Account account = new AliyunAccount("", "");
        Odps odps = new Odps(account);
        odps.setDefaultProject("");
        odps.setEndpoint("http://service-corp.odps.aliyun-inc.com/api");

        // 准备构建SQLExecutor
        SQLExecutorBuilder builder = SQLExecutorBuilder.builder();

        SQLExecutor sqlExecutor = null;
        try {
            // run in offline mode or run in interactive mode
            if (false) {
                // 创建一个默认执行离线SQL的Executor
                sqlExecutor = builder.odps(odps).executeMode(ExecuteMode.OFFLINE).build();
            } else {
                // 创建一个默认执行短查询模式的Executor
                sqlExecutor = builder.odps(odps).executeMode(ExecuteMode.INTERACTIVE).build();
            }
            // 若有需要的话可以传入query的特殊设置
            Map<String, String> queryHint = new HashMap<>();
            queryHint.put("odps.sql.mapper.split.size", "128");
            // 提交一个query,支持传入Hint
            sqlExecutor.run("select count(1) from test_table;", queryHint);

            // 列举一些支持的常用获取信息的接口
            // UUID
            System.out.println("ExecutorId:" + sqlExecutor.getId());
            // 当前query的logview
            System.out.println("Logview:" + sqlExecutor.getLogView());
            // 当前query的Instance对象(Interactive模式多个query可能为同一个instance)
            System.out.println("InstanceId:" + sqlExecutor.getInstance().getId());
            // 当前query的阶段进度(Console的进度条)
            System.out.println("QueryStageProgress:" + sqlExecutor.getProgress());
            // 当前query的执行状态变化日志,比如回退等信息
            System.out.println("QueryExecutionLog:" + sqlExecutor.getExecutionLog());

            // 提供两种获取结果的接口
            if(false) {
                // 直接获取全部query结果,同步接口,可能会占用本线程直到query成功或失败
                List<Record> records = sqlExecutor.getResult();
                printRecords(records);
            } else {
                // 获取query结果的迭代器ResultSet,同步接口,可能会占用本线程直到query成功或失败
                ResultSet resultSet = sqlExecutor.getResultSet();
                while (resultSet.hasNext()) {
                    printRecord(resultSet.next());
                }
            }

            // run another query
            sqlExecutor.run("select * from test_table;", new HashMap<>());
            if(false) {
                // 直接获取全部query结果,同步接口,可能会占用本线程直到query成功或失败
                List<Record> records = sqlExecutor.getResult();
                printRecords(records);
            } else {
                // 获取query结果的迭代器ResultSet,同步接口,可能会占用本线程直到query成功或失败
                ResultSet resultSet = sqlExecutor.getResultSet();
                while (resultSet.hasNext()) {
                    printRecord(resultSet.next());
                }
            }
        } catch (OdpsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (sqlExecutor != null) {
                // 关闭Executor释放相关资源
                sqlExecutor.close();
            }
        }
    }

    // SQLExecutor can be reused by pool mode
    public static void ExampleWithPool() {
        // 设置账号和project信息
        Account account = new AliyunAccount("", "");
        Odps odps = new Odps(account);
        odps.setDefaultProject("");
        odps.setEndpoint("http://service-corp.odps.aliyun-inc.com/api");

        // 通过连接池方式执行query
        SQLExecutorPool sqlExecutorPool = null;
        SQLExecutor sqlExecutor = null;
        try {
            // 准备连接池,设置连接池大小和默认执行模式
            SQLExecutorPoolBuilder builder = SQLExecutorPoolBuilder.builder();
            builder.odps(odps)
                    .initPoolSize(1) // init pool executor number
                    .maxPoolSize(5)  // max executors in pool
                    .executeMode(ExecuteMode.INTERACTIVE); // run in interactive mode

            sqlExecutorPool = builder.build();
            // 从连接池中获取一个Executor,如果不够将会在Max限制内新增Executor
            sqlExecutor = sqlExecutorPool.getExecutor();

            // Executor具体用法和上一例子一致
            sqlExecutor.run("select count(1) from test_table;", new HashMap<>());
            System.out.println("InstanceId:" + sqlExecutor.getId());
            System.out.println("Logview:" + sqlExecutor.getLogView());

            List<Record> records = sqlExecutor.getResult();
            printRecords(records);
        } catch (OdpsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            sqlExecutor.close();
        }
        sqlExecutorPool.close();
    }

    private static void printRecord(Record record) {
        for (int k = 0; k < record.getColumnCount(); k++) {

            if (k != 0) {
                System.out.print("\t");
            }

            if (record.getColumns()[k].getType().equals(OdpsType.STRING)) {
                System.out.print(record.getString(k));
            } else if (record.getColumns()[k].getType().equals(OdpsType.BIGINT)) {
                System.out.print(record.getBigint(k));
            } else {
                System.out.print(record.get(k));
            }
        }
    }

    private static void printRecords(List<Record> records) {
        for (Record record : records) {
            printRecord(record);
            System.out.println();
        }
    }

    public static void main(String args[]) {
        SimpleExample();
        ExampleWithPool();
    }
}