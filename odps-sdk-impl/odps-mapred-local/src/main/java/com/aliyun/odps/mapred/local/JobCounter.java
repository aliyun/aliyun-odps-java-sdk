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

package com.aliyun.odps.mapred.local;

public enum JobCounter {
  MAP_INPUT_RECORDS,
  MAP_SHUFFLE_RECORDS,
  MAP_OUTPUT_RECORDS,
  MAP_SKIPPED_RECORDS,
  MAP_INPUT_BYTES,
  MAP_SHUFFLE_BYTES,
  MAP_OUTPUT_BYTES,
  MAP_MAX_MEMORY,
  MAP_SPILL_FILES,
  MAP_MAX_USED_BUFFER,

  COMBINE_INPUT_RECORDS,
  COMBINE_INPUT_GROUPS,
  COMBINE_OUTPUT_RECORDS,

  REDUCE_INPUT_GROUPS,
  REDUCE_INPUT_RECORDS,
  REDUCE_OUTPUT_BYTES,
  REDUCE_OUTPUT_RECORDS,
  REDUCE_SKIPPED_GROUPS,
  REDUCE_SKIPPED_RECORDS,
  REDUCE_MAX_MEMORY,

  //counter start with __EMPTY will not show in summary
  __EMPTY_WILL_NOT_SHOW,
  __EMPTY_INPUT_RECORD_COUNT,
  __EMPTY_OUTPUT_RECORD_COUNT;

}
