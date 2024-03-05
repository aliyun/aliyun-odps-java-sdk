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

import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.Session;
import com.aliyun.odps.table.read.TableReadSession;
import com.google.gson.JsonObject;

import java.util.*;
import java.util.stream.Collectors;

public class SessionUtils {

    @SuppressWarnings("unchecked")
    public static <T extends Session.Provider> T discoverSessionProvider(
            ClassLoader classLoader, Class<T> providerClass, String providerIdentifier) throws ClassNotFoundException {
        final List<Session.Provider> factories = discoverProviders(classLoader);

        final List<Session.Provider> foundProviders =
                factories.stream()
                        .filter(f -> providerClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        if (foundProviders.isEmpty()) {
            throw new ClassNotFoundException(
                    String.format(
                            "Could not find any provider that implement '%s' in the classpath.",
                            providerClass.getName()));
        }

        final List<TableReadSession.Provider> matchingProviders =
                foundProviders.stream()
                        .filter(f -> f.identifier().equals(providerIdentifier))
                        .collect(Collectors.toList());

        if (matchingProviders.isEmpty()) {
            throw new ClassNotFoundException(
                    String.format(
                            "Could not find any provider for identifier '%s' that implements '%s' in the classpath.\n\n"
                                    + "Available provider identifiers are:\n\n"
                                    + "%s",
                            providerIdentifier,
                            providerClass.getName(),
                            foundProviders.stream()
                                    .map(Session.Provider::identifier)
                                    .distinct()
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }

        if (matchingProviders.size() > 1) {
            throw new RuntimeException(
                    String.format(
                            "Multiple providers for identifier '%s' that implement '%s' found in the classpath.\n\n"
                                    + "Ambiguous provider classes are:\n\n"
                                    + "%s",
                            providerIdentifier,
                            providerClass.getName(),
                            matchingProviders.stream()
                                    .map(f -> f.getClass().getName())
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
        return (T) matchingProviders.get(0);
    }

    public static List<Session.Provider> discoverProviders(ClassLoader classLoader) {
        final List<Session.Provider> result = new LinkedList<>();
        ServiceLoader<Session.Provider> serviceLoader = ServiceLoader.load(Session.Provider.class, classLoader);
        for (Session.Provider provider : serviceLoader) {
            result.add(provider);
        }
        return result;
    }

    public static DataFormat parseDataFormat(JsonObject format) {
        DataFormat requiredDataFormat = new DataFormat();
        if (format.has("Type")) {
            requiredDataFormat.setType(
                    DataFormat.Type.byTypeName(format.get("Type").getAsString().toUpperCase()).orElse(DataFormat.Type.UNKNOWN));
        }
        if (format.has("Version")) {
            requiredDataFormat.setVersion(
                    DataFormat.Version.byCode(format.get("Version").getAsString().toUpperCase()).orElse(DataFormat.Version.UNKNOWN));
        }
        return requiredDataFormat;
    }
}
