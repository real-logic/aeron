/*
 * Copyright 2014-2022 Real Logic Limited.
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
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NameResolver;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

import java.net.InetAddress;
import java.util.Map;
import java.util.Objects;

import static io.aeron.Aeron.NULL_VALUE;

public class RedirectingNameResolver implements NameResolver
{
    public static final int DISABLE_RESOLUTION = -1;
    public static final int USE_INITIAL_RESOLUTION_HOST = 0;
    public static final int USE_RE_RESOLUTION_HOST = 1;
    public static final int NAME_ENTRY_COUNTER_TYPE_ID = 2001;
    public static final int EXPECTED_COLUMN_COUNT = 3;
    private static final String INVALID_HOSTNAME_SENTINEL = "forced-resolve-failure.invalid";

    private final Map<String, NameEntry> nameToEntryMap = new Object2ObjectHashMap<>();
    private final String csvConfiguration;

    public RedirectingNameResolver(final String csvConfiguration)
    {
        this.csvConfiguration = csvConfiguration;
        final String[] lines = csvConfiguration.split("\\|");
        for (final String line : lines)
        {
            final String[] params = line.split(",");
            if (EXPECTED_COLUMN_COUNT != params.length)
            {
                throw new IllegalArgumentException("Expected 3 elements per row");
            }

            final NameEntry nameEntry = new NameEntry(params[0], params[1], params[2]);
            nameToEntryMap.put(nameEntry.name, nameEntry);
        }
    }

    public void init(final MediaDriver.Context context)
    {
        final CountersManager countersManager = context.countersManager();

        for (final NameEntry nameEntry : nameToEntryMap.values())
        {
            final AtomicCounter atomicCounter = countersManager.newCounter(
                nameEntry.toString(),
                NAME_ENTRY_COUNTER_TYPE_ID,
                (mutableDirectBuffer) -> mutableDirectBuffer.putStringAscii(0, nameEntry.name));
            nameEntry.counter(atomicCounter);
        }
    }

    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        final NameEntry nameEntry = nameToEntryMap.get(name);
        final String hostname = null != nameEntry ? nameEntry.redirectHost(name) : name;

        InetAddress resolvedAddress = null;
        if (!Objects.equals(INVALID_HOSTNAME_SENTINEL, hostname))
        {
            resolvedAddress = DefaultNameResolver.INSTANCE.resolve(hostname, uriParamName, isReResolution);
        }

        DefaultNameResolver.INSTANCE.resolveHook(this.getClass().getSimpleName(), hostname, resolvedAddress);

        return resolvedAddress;
    }

    public String csvConfiguration()
    {
        return csvConfiguration;
    }

    public static boolean updateNameResolutionStatus(
        final CountersReader counters,
        final String hostname,
        final int operationValue)
    {
        final MutableInteger nameCounterId = new MutableInteger(NULL_VALUE);
        counters.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (typeId == NAME_ENTRY_COUNTER_TYPE_ID && hostname.equals(keyBuffer.getStringAscii(0)))
            {
                nameCounterId.set(counterId);
            }
        });

        final boolean counterFound = NULL_VALUE != nameCounterId.get();
        if (counterFound)
        {
            final AtomicCounter nameCounter = new AtomicCounter(counters.valuesBuffer(), nameCounterId.get());

            nameCounter.set(operationValue);
        }

        return counterFound;
    }

    static final class NameEntry
    {
        private final String name;
        private final String initialResolutionHost;
        private final String reResolutionHost;
        private AtomicCounter counter;

        NameEntry(final String name, final String initialResolutionHost, final String reResolutionHost)
        {
            this.name = name;
            this.initialResolutionHost = initialResolutionHost;
            this.reResolutionHost = reResolutionHost;
        }

        void counter(final AtomicCounter counter)
        {
            this.counter = counter;
        }

        String redirectHost(final String name)
        {
            final long operation = counter.get();
            if (DISABLE_RESOLUTION == operation)
            {
                return INVALID_HOSTNAME_SENTINEL;
            }
            else if (USE_INITIAL_RESOLUTION_HOST == operation)
            {
                return initialResolutionHost;
            }
            else if (USE_RE_RESOLUTION_HOST == operation)
            {
                return reResolutionHost;
            }
            else
            {
                return name;
            }
        }

        public String toString()
        {
            return "NameEntry{" +
                "name='" + name + '\'' +
                ", initialResolutionHost='" + initialResolutionHost + '\'' +
                ", reResolutionHost='" + reResolutionHost + "'}";
        }
    }
}
