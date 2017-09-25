/*
 * Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.SystemTestHelper.spyForChannel;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests that allow Spies to simulate connection
 */
@RunWith(Theories.class)
public class SpySimulateConnectionTest
{
    @DataPoint
    public static final String UNICAST_CHANNEL = "aeron:udp?endpoint=localhost:54325";

    @DataPoint
    public static final String MULTICAST_CHANNEL = "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost";

    private static final int STREAM_ID = 1;

    private static final int TERM_BUFFER_LENGTH = 64 * 1024;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;

    private final MediaDriver.Context driverContext = new MediaDriver.Context();

    private Aeron client;
    private MediaDriver driver;
    private Publication publication;
    private Subscription subscription;
    private Subscription spy;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private FragmentHandler fragmentHandlerSpy = mock(FragmentHandler.class);
    private FragmentHandler fragmentHandlerSubscription = mock(FragmentHandler.class);

    private void launch()
    {
        driverContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .sharedIdleStrategy(new YieldingIdleStrategy())
            .threadingMode(ThreadingMode.SHARED);

        driver = MediaDriver.launch(driverContext);
        client = Aeron.connect(new Aeron.Context());
    }

    @After
    public void closeEverything()
    {
        CloseHelper.quietClose(publication);
        CloseHelper.quietClose(subscription);
        CloseHelper.quietClose(spy);

        CloseHelper.quietClose(client);
        CloseHelper.quietClose(driver);

        IoUtil.delete(driverContext.aeronDirectory(), true);
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldNotSimulateConnectionWhenNotConfiguredTo(final String channel) throws Exception
    {
        launch();

        publication = client.addPublication(channel, STREAM_ID);
        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);

        while (spy.hasNoImages())
        {
            Thread.yield();
        }

        assertFalse(publication.isConnected());
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldSimulateConnectionWithNoNetworkSubscriptions(final String channel) throws Exception
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        driverContext.publicationConnectionTimeoutNs(TimeUnit.SECONDS.toNanos(1));
        driverContext.spiesSimulateConnection(true);

        launch();

        publication = client.addPublication(channel, STREAM_ID);
        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);

        while (spy.hasNoImages())
        {
            Thread.yield();
        }

        while (!publication.isConnected())
        {
            Thread.yield();
        }

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Thread.yield();
            }

            final AtomicInteger fragmentsRead = new AtomicInteger();
            SystemTestHelper.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.addAndGet(spy.poll(fragmentHandlerSpy, 10));
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(fragmentHandlerSpy, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldSimulateConnectionWithSlowNetworkSubscription(final String channel) throws Exception
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int numMessagesLeftToSend = numMessagesToSend;
        int numFragmentsFromSpy = 0;
        int numFragmentsFromSubscription = 0;

        driverContext.publicationConnectionTimeoutNs(TimeUnit.SECONDS.toNanos(1));
        driverContext.spiesSimulateConnection(true);

        launch();

        publication = client.addPublication(channel, STREAM_ID);
        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        subscription = client.addSubscription(channel, STREAM_ID);

        while (spy.hasNoImages() || subscription.hasNoImages())
        {
            Thread.yield();
        }

        while (!publication.isConnected())
        {
            Thread.yield();
        }

        for (long i = 0;
             numFragmentsFromSpy < numMessagesToSend || numFragmentsFromSubscription < numMessagesToSend;
             i++)
        {
            if (numMessagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    numMessagesLeftToSend--;
                }
            }

            Thread.yield();

            numFragmentsFromSpy += spy.poll(fragmentHandlerSpy, 10);

            // subscription receives slowly
            if ((i % 2) == 0)
            {
                numFragmentsFromSubscription += subscription.poll(fragmentHandlerSubscription, 1);
            }
        }

        verify(fragmentHandlerSpy, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));

        verify(fragmentHandlerSubscription, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldSimulateConnectionWithLeavingNetworkSubscription(final String channel) throws Exception
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int numMessagesLeftToSend = numMessagesToSend;
        int numFragmentsReadFromSpy = 0;
        int numFragmentsReadFromSubscription = 0;
        boolean isSubscriptionClosed = false;

        driverContext.publicationConnectionTimeoutNs(TimeUnit.SECONDS.toNanos(1));
        driverContext.spiesSimulateConnection(true);

        launch();

        publication = client.addPublication(channel, STREAM_ID);
        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        subscription = client.addSubscription(channel, STREAM_ID);

        while (spy.hasNoImages() || subscription.hasNoImages())
        {
            Thread.yield();
        }

        while (!publication.isConnected())
        {
            Thread.yield();
        }

        while (numFragmentsReadFromSpy < numMessagesToSend)
        {
            if (numMessagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    numMessagesLeftToSend--;
                }
            }

            numFragmentsReadFromSpy += spy.poll(fragmentHandlerSpy, 10);

            // subscription receives up to 1/8 of the messages, then stops
            if (numFragmentsReadFromSubscription < (numMessagesToSend / 8))
            {
                numFragmentsReadFromSubscription += subscription.poll(fragmentHandlerSubscription, 10);
            }
            else if (!isSubscriptionClosed)
            {
                subscription.close();
                isSubscriptionClosed = true;
            }
        }

        verify(fragmentHandlerSpy, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }
}
