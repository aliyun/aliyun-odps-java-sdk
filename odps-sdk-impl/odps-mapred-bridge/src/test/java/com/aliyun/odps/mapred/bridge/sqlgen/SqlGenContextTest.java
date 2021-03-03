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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.aliyun.odps.mapred.conf.BridgeJobConf;
import com.aliyun.odps.mapred.utils.SchemaUtils;
import org.junit.Test;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public class SqlGenContextTest {

    @Test
    public void testMergeTableInfos() {
        JobConf conf = new BridgeJobConf();
        TableInfo table_in1 = TableInfo.builder().projectName("test").tableName("wc_in1").build();
        ((BridgeJobConf) conf).setInputSchema(table_in1, SchemaUtils.fromString("word:string,count:bigint"));
        InputUtils.addTable(table_in1, conf);

        // add wc_in2|a=1/b=1 and wc_in2|a=2/b=2
        for (int i = 1; i <= 2; i++) {
            LinkedHashMap<String, String> parts = new LinkedHashMap<>();
            parts.put("a", String.valueOf(i));
            parts.put("b", String.valueOf(i));
            TableInfo table_in2 = TableInfo.builder().projectName("test").tableName("wc_in2").partSpec(parts).build();
            ((BridgeJobConf) conf).setInputSchema(table_in2, SchemaUtils.fromString("word:string,count:bigint"));
            InputUtils.addTable(table_in2, conf);
        }

        SqlGenContext context = new SqlGenContext(conf, "");
        LinkedHashMap<TableInfo, List<String>> infos = context.mergeTableInfos();
        assertEquals(2, infos.size());

        Iterator<TableInfo> it = infos.keySet().iterator();
        assertEquals("wc_in1", it.next().getTableName());

        TableInfo table = it.next();
        assertEquals("wc_in2", table.getTableName());
        List<String> partitions = infos.get(table);
        assertEquals(2, partitions.size());
        assertEquals("a = \"1\" AND b = \"1\"", partitions.get(0));
        assertEquals("a = \"2\" AND b = \"2\"", partitions.get(1));
    }

}
