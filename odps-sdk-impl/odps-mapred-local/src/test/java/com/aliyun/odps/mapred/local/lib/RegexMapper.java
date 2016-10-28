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

package com.aliyun.odps.mapred.local.lib;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;

/**
 * RegexMapper
 *
 * @author mingdi
 */
public class RegexMapper extends MapperBase {

  private Pattern pattern;
  private int group;

  private Record word;
  private Record one;

  @Override
  public void setup(TaskContext context) throws IOException {
    JobConf job = (JobConf) context.getJobConf();
    pattern = Pattern.compile(job.get("mapred.mapper.regex"));
    group = job.getInt("mapred.mapper.regex.group", 0);

    word = context.createMapOutputKeyRecord();
    one = context.createMapOutputValueRecord();
    one.set(new Object[]{1L});
  }

  @Override
  public void map(long recordNum, Record record, TaskContext context)
      throws IOException {
    for (int i = 0; i < record.getColumnCount(); ++i) {
      String text = record.get(i).toString();
      Matcher matcher = pattern.matcher(text);
      while (matcher.find()) {
        word.set(new Object[]{matcher.group(group)});
        context.write(word, one);
      }
    }
  }

  // @Override
  // protected void setup(MapContext<Text, LongWritable> context)
  // throws IOException, InterruptedException {
  // JobConf job = (JobConf) context.getConfiguration();
  // pattern = Pattern.compile(job.get("mapred.mapper.regex"));
  // group = job.getInt("mapred.mapper.regex.group", 0);
  // }
  //
  // @Override
  // public void map(LongWritable key, Record value,
  // MapContext<Text, LongWritable> context) throws IOException,
  // InterruptedException {
  //
  // for (int i = 0; i < value.size(); ++i) {
  // String text = ((Text) value.get(i)).toString();
  // Matcher matcher = pattern.matcher(text);
  // while (matcher.find()) {
  // context.write(new Text(matcher.group(group)), new LongWritable(1));
  // }
  // }
  // }
}
