/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron;

import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NameResolver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class NameReResolutionTest
{
    private static final String ENDPOINT_NAME = "ReResTest";
    private static final String PUBLICATION_MDC_URI = "aeron:udp?control=localhost:24327|control-mode=manual";
    private static final String PUBLICATION_URI = "aeron:udp?endpoint=" + ENDPOINT_NAME;
    private static final String FIRST_SUBSCRIPTION_URI = "aeron:udp?endpoint=localhost:24325";
    private static final String SECOND_SUBSCRIPTION_URI = "aeron:udp?endpoint=localhost:24326";

    private static final int STREAM_ID = 1001;

    private Aeron client;
    private MediaDriver driver;
    private Subscription firstSubscription;
    private Subscription secondSubscription;
    private Publication publication;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private final FragmentHandler handler = mock(FragmentHandler.class);

    private final NameResolver lookupResolver = new NameResolver()
    {
        public InetAddress resolve(final CharSequence name, final String uriParamName, final boolean isReResolution)
        {
            return DefaultNameResolver.INSTANCE.resolve(name, uriParamName, isReResolution);
        }

        public CharSequence lookup(final CharSequence name, final String uriParamName, final boolean isReLookup)
        {
            if (name.equals(ENDPOINT_NAME))
            {
                if (isReLookup)
                {
                    return "localhost:" + 24326;
                }
                else
                {
                    return "localhost:" + 24325;
                }
            }

            return name;
        }
    };

    @BeforeEach
    public void before()
    {
        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .errorHandler(Throwable::printStackTrace)
                .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                .threadingMode(ThreadingMode.SHARED)
                .nameResolver(lookupResolver));

        client = Aeron.connect();
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(client, driver);
        driver.context().deleteDirectory();
    }

    @SlowTest
    @Test
    @Timeout(10)
    public void shouldReResolveEndpointOnNoConnected() throws Exception
    {
        buffer.putInt(0, 1);

        firstSubscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_URI, STREAM_ID);

        while (!firstSubscription.isConnected())
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        SystemTests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += firstSubscription.poll(handler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        // close first subscription
        firstSubscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Thread.sleep(100);
            Tests.checkInterruptStatus();
        }

        secondSubscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);

        while (!secondSubscription.isConnected())
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        SystemTests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += secondSubscription.poll(handler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }

    @SlowTest
    @Test
    @Timeout(10)
    public void shouldReResolveMdcManualEndpointOnNoConnected() throws Exception
    {
        buffer.putInt(0, 1);

        firstSubscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_MDC_URI, STREAM_ID);
        publication.addDestination(PUBLICATION_URI);

        while (!firstSubscription.isConnected())
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        SystemTests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += firstSubscription.poll(handler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        // close first subscription
        firstSubscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Thread.sleep(100);
            Tests.checkInterruptStatus();
        }

        secondSubscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);

        while (!secondSubscription.isConnected())
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        while (publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Thread.yield();
            Tests.checkInterruptStatus();
        }

        SystemTests.executeUntil(
            () -> fragmentsRead.get() > 0,
            (i) ->
            {
                fragmentsRead.value += secondSubscription.poll(handler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        verify(handler, times(2)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(BitUtil.SIZE_OF_INT),
            any(Header.class));
    }
}
