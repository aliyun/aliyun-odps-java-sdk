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

package com.aliyun.odps.local.common;

public class ExceptionCode {

  // //////////// Exception Code in mr_task //////////////
  public static final String ODPS_0110999 = "ODPS-0110999: Critical";

  public static final String
      ODPS_0710005 =
      "ODPS-0710005: Exception occurred when connecting to meta store";
  public static final String
      ODPS_0710015 =
      "ODPS-0710015: MR task encountered meta store exception";
  public static final String ODPS_0710025 = "ODPS-0710025: Get partitions from meta store error";

  public static final String ODPS_0720001 = "ODPS-0720001: Invalid table name format";
  public static final String
      ODPS_0720015 =
      "ODPS-0720015: PartKeys size do not match partVals size";
  public static final String ODPS_0720021 = "ODPS-0720021: Column does not exist";
  public static final String
      ODPS_0720031 =
      "ODPS-0720031: Cache resource between comma must not be empty";
  public static final String ODPS_0720041 = "ODPS-0720041: Resource not found";
  public static final String ODPS_0720051 = "ODPS-0720051: Resource table should not be a view";
  public static final String
      ODPS_0720061 =
      "ODPS-0720061: Table or partition not found for resource";
  public static final String
      ODPS_0720071 =
      "ODPS-0720071: Total size of cache resources is too big";
  public static final String ODPS_0720081 = "ODPS-0720081: Job has not specified mapper class";
  public static final String ODPS_0720091 = "ODPS-0720091: Column duplicate";
  public static final String ODPS_0720101 = "ODPS-0720101: Input table should not be a view";
  public static final String ODPS_0720111 = "ODPS-0720111: Output table should not be a view";
  public static final String ODPS_0720121 = "ODPS-0720121: Invalid table partSpec";
  public static final String ODPS_0720131 = "ODPS-0720131: Invalid multiple output";
  public static final String ODPS_0720141 = "ODPS-0720141: Memory value out of bound";
  public static final String ODPS_0720151 = "ODPS-0720151: Cpu value out of bound";
  public static final String ODPS_0720161 = "ODPS-0720161: Invalid max attempts value";
  public static final String ODPS_0720171 = "ODPS-0720171: Invalid IO sort buffer";
  public static final String
      ODPS_0720181 =
      "ODPS-0720181: Classpath resource between comma must not be empty";
  public static final String ODPS_0720191 = "ODPS-0720191: Invalid input split mode";
  public static final String ODPS_0720201 = "ODPS-0720201: Invalid map split size";
  public static final String ODPS_0720211 = "ODPS-0720211: Invalid number of map tasks";
  public static final String ODPS_0720221 = "ODPS-0720221: Invalid max splits number";
  public static final String ODPS_0720231 = "ODPS-0720231: Job input not set";
  public static final String ODPS_0720241 = "ODPS-0720241: Num of map instance is too big";
  public static final String ODPS_0720251 = "ODPS-0720251: Num of reduce instance is invalid";
  public static final String ODPS_0720261 = "ODPS-0720261: Invalid partition value";
  public static final String
      ODPS_0720271 =
      "ODPS-0720271: Allow no input is conflict to split mode";
  public static final String ODPS_0720281 = "ODPS-0720281: Invalid partitition format";
  public static final String ODPS_0720291 = "ODPS-0720291: Invalid description json";
  public static final String ODPS_0720301 = "ODPS-0720301: Too many job input";
  public static final String ODPS_0720311 = "ODPS-0720311: Invalid output label";
  public static final String ODPS_0720321 = "ODPS-0720321: Too many job output";
  public static final String ODPS_0720331 = "ODPS-0720331: Too many cache resources";
  public static final String
      ODPS_0720341 =
      "ODPS-0720341: Can't cancel job when job is finished or doing ddl task";
  public static final String ODPS_0720351 = "ODPS-0720351: Invalid job log level";
  public static final String ODPS_0720361 = "ODPS-0720361: Fuxi job got canceled";
  public static final String ODPS_0720371 = "ODPS-0720371: Task canceled";
  public static final String ODPS_0720381 = "ODPS-0720381: Invalid project";

  public static final String ODPS_0720411 = "ODPS-0720411: Invalid resource format";
  public static final String
      ODPS_0720431 =
      "ODPS-0720431: Classpath resource not found in resource list";
  public static final String ODPS_0720441 = "ODPS-0720441: Invalid alias name";

  public static final String ODPS_0730005 = "ODPS-0730005: MR job internal error";

  /* mr local-run exception code */
  public static final String ODPS_0740001 = "ODPS-0740001: Too many local-run maps";

  // //////////// Exception Code in Java Side //////////////

}
