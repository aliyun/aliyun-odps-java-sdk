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

package com.aliyun.odps.table.utils;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A collection of static utility methods to validate input.
 */
public final class Preconditions {

    public static <T> T checkNotNull(@Nullable T reference, String argName) {
        if (reference == null) {
            throw new IllegalArgumentException(argName + " == null!");
        }
        return reference;
    }

    public static <T> T checkNotNull(@Nullable T reference,
                                     String argName,
                                     String errorMessage) {
        if (reference == null) {
            throw new IllegalArgumentException(argName + ":" + errorMessage);
        }
        return reference;
    }

    public static void checkArgument(boolean condition, String errorMessage) {
        if (!condition) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    public static void checkString(String arg, String name) {
        if (arg == null || arg.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty string!");
        }
    }

    public static void checkList(List<?> arg, String name) {
        checkList(arg, 1, name);
    }

    public static void checkList(List<?> arg, int minSize, String name) {
        checkNotNull(arg, name);
        if (arg.size() < minSize) {
            throw new IllegalArgumentException(name + " must has at least " + minSize + "items");
        }
        for (Object item : arg) {
            checkNotNull(item, name + "[x]");
        }
    }

    public static void checkInteger(Integer arg, int minValue, String name) {
        checkNotNull(arg, name);
        if (arg < minValue) {
            throw new IllegalArgumentException(name + " < " + minValue);
        }
    }

    public static void checkLong(Long arg, long minValue, String name) {
        checkNotNull(arg, name);
        if (arg < minValue) {
            throw new IllegalArgumentException(name + " < " + minValue);
        }
    }

    public static void checkIntList(List<Integer> arg,
                                    int minSize,
                                    int minValue,
                                    String name) {
        checkNotNull(arg, name);
        if (arg.size() < minSize) {
            throw new IllegalArgumentException(name + " must has at least " + minSize + "items");
        }
        for (Integer value : arg) {
            checkInteger(value, minValue, name + "[x]");
        }
    }

    private Preconditions() {
    }

}
