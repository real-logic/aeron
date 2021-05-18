/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.test.driver;

import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.NameResolver;
import org.agrona.collections.Object2ObjectHashMap;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

public class RedirectingNameResolver implements NameResolver
{
    private final Map<String, Map<String, NameEntry>> paramNameToNameMap = new Object2ObjectHashMap<>();

    public RedirectingNameResolver(final String csvConfiguration)
    {
        final String[] lines = csvConfiguration.split("\\|");
        for (final String line : lines)
        {
            final String[] params = line.split(",");
            if (4 != params.length)
            {
                throw new IllegalArgumentException("Expect 4 elements per row");
            }

            final NameEntry nameEntry = new NameEntry(params[0], params[1], params[2], params[3]);
            paramNameToNameMap.computeIfAbsent(
                nameEntry.paramName, ignore -> new Object2ObjectHashMap<>()).put(nameEntry.name, nameEntry);
        }
    }

    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        final NameEntry nameEntry = paramNameToNameMap.getOrDefault(uriParamName, Collections.emptyMap()).get(name);
        final String hostname;
        if (null != nameEntry)
        {
            hostname = isReResolution ? nameEntry.reResolutionHost : nameEntry.initialResolutionHost;
        }
        else
        {
            hostname = name;
        }

        return DefaultNameResolver.INSTANCE.resolve(hostname, uriParamName, isReResolution);
    }

    private static final class NameEntry
    {
        final String paramName;
        final String name;
        final String initialResolutionHost;
        final String reResolutionHost;

        NameEntry(
            final String paramName,
            final String name,
            final String initialResolutionHost,
            final String reResolutionHost)
        {
            this.paramName = paramName;
            this.name = name;
            this.initialResolutionHost = initialResolutionHost;
            this.reResolutionHost = reResolutionHost;
        }
    }
}
