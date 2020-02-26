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

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.MDC_CONTROL_PARAM_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class NameReResolutionTest
{
    private static final String ENDPOINT_NAME = "ReResTestEndpoint";
    private static final String PUBLICATION_MANUAL_MDC_URI = "aeron:udp?control=localhost:24327|control-mode=manual";
    private static final String PUBLICATION_URI = "aeron:udp?endpoint=" + ENDPOINT_NAME;
    private static final String FIRST_SUBSCRIPTION_URI = "aeron:udp?endpoint=localhost:24325";
    private static final String SECOND_SUBSCRIPTION_URI = "aeron:udp?endpoint=localhost:24326";

    private static final String CONTROL_NAME = "ReResTestControl";
    private static final String FIRST_PUBLICATION_DYNAMIC_MDC_URI =
        "aeron:udp?control=localhost:24327|control-mode=dynamic|linger=0";
    private static final String SECOND_PUBLICATION_DYNAMIC_MDC_URI =
        "aeron:udp?control=localhost:24328|control-mode=dynamic";
    private static final String SUBSCRIPTION_DYNAMIC_MDC_URI =
        "aeron:udp?control=" + CONTROL_NAME + "|control-mode=dynamic";
    private static final String SUBSCRIPTION_MDS_URI = "aeron:udp?control-mode=manual";

    private static final int STREAM_ID = 1001;

    private Aeron client;
    private MediaDriver driver;
    private Subscription subscription;
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
            if (name.equals(ENDPOINT_NAME) && uriParamName.equals(ENDPOINT_PARAM_NAME))
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
            else if (name.equals(CONTROL_NAME) && uriParamName.equals(MDC_CONTROL_PARAM_NAME))
            {
                if (isReLookup)
                {
                    return "localhost:" + 24328;
                }
                else
                {
                    return "localhost:" + 24327;
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

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_URI, STREAM_ID);

        while (!subscription.isConnected())
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
                fragmentsRead.value += subscription.poll(handler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Thread.sleep(100);
            Tests.checkInterruptStatus();
        }

        subscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);

        while (!subscription.isConnected())
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
                fragmentsRead.value += subscription.poll(handler, 1);
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

        subscription = client.addSubscription(FIRST_SUBSCRIPTION_URI, STREAM_ID);
        publication = client.addPublication(PUBLICATION_MANUAL_MDC_URI, STREAM_ID);
        publication.addDestination(PUBLICATION_URI);

        while (!subscription.isConnected())
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
                fragmentsRead.value += subscription.poll(handler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        subscription.close();

        // wait for disconnect to ensure we stay in lock step
        while (publication.isConnected())
        {
            Thread.sleep(100);
            Tests.checkInterruptStatus();
        }

        subscription = client.addSubscription(SECOND_SUBSCRIPTION_URI, STREAM_ID);

        while (!subscription.isConnected())
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
                fragmentsRead.value += subscription.poll(handler, 1);
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
    @Timeout(15)
    public void shouldReResolveMdcDynamicControlOnNoConnected() throws Exception
    {
        buffer.putInt(0, 1);

        subscription = client.addSubscription(SUBSCRIPTION_DYNAMIC_MDC_URI, STREAM_ID);
        publication = client.addPublication(FIRST_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
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
                fragmentsRead.value += subscription.poll(handler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        publication.close();

        // wait for disconnect to ensure we stay in lock step
        while (subscription.isConnected())
        {
            Thread.sleep(100);
            Tests.checkInterruptStatus();
        }

        publication = client.addPublication(SECOND_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!publication.isConnected())
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
                fragmentsRead.value += subscription.poll(handler, 1);
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
    @Timeout(15)
    public void shouldReResolveMdcDynamicControlOnManualDestinationSubscriptionOnNoConnected() throws Exception
    {
        buffer.putInt(0, 1);

        subscription = client.addSubscription(SUBSCRIPTION_MDS_URI, STREAM_ID);
        subscription.addDestination(SUBSCRIPTION_DYNAMIC_MDC_URI);
        publication = client.addPublication(FIRST_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!subscription.isConnected())
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
                fragmentsRead.value += subscription.poll(handler, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(5900));

        fragmentsRead.set(0);

        publication.close();

        // wait for disconnect to ensure we stay in lock step
        while (subscription.isConnected())
        {
            Thread.sleep(100);
            Tests.checkInterruptStatus();
        }

        publication = client.addPublication(SECOND_PUBLICATION_DYNAMIC_MDC_URI, STREAM_ID);

        while (!publication.isConnected())
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
                fragmentsRead.value += subscription.poll(handler, 1);
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
