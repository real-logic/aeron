/*
 * Copyright 2014-2023 Real Logic Limited.
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

import org.agrona.CloseHelper;
import org.agrona.concurrent.NanoClock;

import java.net.InetAddress;

import static java.util.Objects.requireNonNull;

final class TimeTrackingNameResolver implements NameResolver, AutoCloseable
{
    private final NameResolver delegateResolver;
    private final NanoClock clock;
    private final DutyCycleTracker maxTimeTracker;

    TimeTrackingNameResolver(
        final NameResolver delegateResolver,
        final NanoClock clock,
        final DutyCycleTracker maxTimeTracker)
    {
        this.delegateResolver = requireNonNull(delegateResolver);
        this.clock = requireNonNull(clock);
        this.maxTimeTracker = requireNonNull(maxTimeTracker);
    }

    /**
     * {@inheritDoc}
     */
    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        final long beginNs = clock.nanoTime();
        maxTimeTracker.update(beginNs);
        InetAddress address = null;
        try
        {
            address = delegateResolver.resolve(name, uriParamName, isReResolution);
            return address;
        }
        finally
        {
            final long endNs = clock.nanoTime();
            maxTimeTracker.measureAndUpdate(endNs);
            DefaultNameResolver.logResolve(delegateResolver.getClass().getSimpleName(), endNs - beginNs, name, address);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String lookup(final String name, final String uriParamName, final boolean isReLookup)
    {
        return delegateResolver.lookup(name, uriParamName, isReLookup);
    }

    /**
     * {@inheritDoc}
     */
    public void init(final MediaDriver.Context context)
    {
        delegateResolver.init(context);
    }

    /**
     * {@inheritDoc}
     */
    public int doWork(final long nowMs)
    {
        return delegateResolver.doWork(nowMs);
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (delegateResolver instanceof AutoCloseable)
        {
            CloseHelper.close((AutoCloseable)delegateResolver);
        }
    }
}
