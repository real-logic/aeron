/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.driver;

import io.aeron.CounterProvider;
import org.agrona.concurrent.status.CountersReader;

import java.net.InetAddress;

/**
 * Interface to allow resolving a name to an {@link InetAddress}.
 */
public interface NameResolver
{
    /**
     * Resolve a name and return the most up to date {@link InetAddress} that represents the name.
     *
     * @param name           to resolve.
     * @param uriParamName   that the resolution is for.
     * @param isReResolution {@code true} if this is a re-resolution or {@code false} if initial resolution.
     * @return address for the name that most recently represents the name or null if not resolvable currently.
     * @see io.aeron.CommonContext#ENDPOINT_PARAM_NAME
     * @see io.aeron.CommonContext#MDC_CONTROL_PARAM_NAME
     */
    InetAddress resolve(String name, String uriParamName, boolean isReResolution);

    /**
     * Lookup the name and return a string of the form name:port that represents the endpoint or control param
     * of the URI.
     *
     * @param name         to lookup
     * @param uriParamName that the lookup is for.
     * @param isReLookup   {@code true} if this is a re-lookup or {@code true} if initial lookup.
     * @return string in name:port form.
     */
    default String lookup(String name, String uriParamName, boolean isReLookup)
    {
        return name;
    }

    /**
     * Do post construction initialisation of the name resolver.  Happens during the conductor start lifecycle.  Can be
     * used for actions like adding counters.
     *
     * @param context for the media driver that the name resolver is running in.
     * @deprecated Use {@link #init(CountersReader, CounterProvider)} instead.
     * @see #init(CountersReader, CounterProvider)
     */
    @Deprecated
    default void init(MediaDriver.Context context)
    {
        throw new UnsupportedOperationException("deprecated: use NameResolver.init(io.aeron.CounterFactory) instead");
    }

    /**
     * Do post construction initialisation of the name resolver.
     *
     * @param countersReader for finding existing counters.
     * @param counterProvider for adding counters.
     */
    default void init(final CountersReader countersReader, CounterProvider counterProvider)
    {
    }

    /**
     * Perform periodic work for the resolver.
     *
     * @param nowMs current epoch clock time in milliseconds
     * @return work count
     */
    default int doWork(final long nowMs)
    {
        return 0;
    }

    /**
     * Gets the name of the resolver, used for logging and debugging.
     *
     * @return name of the resolver, defaults to the qualified class name.
     */
    default String name()
    {
        return this.getClass().getName();
    }
}
