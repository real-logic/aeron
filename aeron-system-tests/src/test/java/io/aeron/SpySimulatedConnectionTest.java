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
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

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
public class SpySimulatedConnectionTest
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
            .errorHandler(Throwable::printStackTrace)
            .threadingMode(ThreadingMode.SHARED);

        driver = MediaDriver.launch(driverContext);
        client = Aeron.connect(new Aeron.Context());
    }

    @After
    public void after()
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
    public void shouldNotSimulateConnectionWhenNotConfiguredTo(final String channel)
    {
        launch();

        publication = client.addPublication(channel, STREAM_ID);
        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);

        while (!spy.isConnected())
        {
            Thread.yield();
        }

        assertFalse(publication.isConnected());
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldSimulateConnectionWithNoNetworkSubscriptions(final String channel)
    {
        final int messagesToSend = NUM_MESSAGES_PER_TERM * 3;

        driverContext
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(250))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .spiesSimulateConnection(true);

        launch();

        publication = client.addPublication(channel, STREAM_ID);
        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);

        while (!spy.isConnected() || !publication.isConnected())
        {
            Thread.yield();
        }

        for (int i = 0; i < messagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();
            SystemTestHelper.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    final int fragments = spy.poll(fragmentHandlerSpy, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(fragmentHandlerSpy, times(messagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldSimulateConnectionWithSlowNetworkSubscription(final String channel)
    {
        final int messagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int messagesLeftToSend = messagesToSend;
        int fragmentsFromSpy = 0;
        int fragmentsFromSubscription = 0;

        driverContext
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(250))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .spiesSimulateConnection(true);

        launch();

        publication = client.addPublication(channel, STREAM_ID);
        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        subscription = client.addSubscription(channel, STREAM_ID);

        while (!spy.isConnected() || !subscription.isConnected() || !publication.isConnected())
        {
            Thread.yield();
        }

        for (int i = 0; fragmentsFromSpy < messagesToSend || fragmentsFromSubscription < messagesToSend; i++)
        {
            if (messagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    messagesLeftToSend--;
                }
            }

            Thread.yield();

            fragmentsFromSpy += spy.poll(fragmentHandlerSpy, 10);

            // subscription receives slowly
            if ((i % 2) == 0)
            {
                fragmentsFromSubscription += subscription.poll(fragmentHandlerSubscription, 1);
            }
        }

        verify(fragmentHandlerSpy, times(messagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));

        verify(fragmentHandlerSubscription, times(messagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldSimulateConnectionWithLeavingNetworkSubscription(final String channel)
    {
        final int messagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int messagesLeftToSend = messagesToSend;
        int fragmentsReadFromSpy = 0;
        int fragmentsReadFromSubscription = 0;
        boolean isSubscriptionClosed = false;

        driverContext
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(250))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .spiesSimulateConnection(true);

        launch();

        publication = client.addPublication(channel, STREAM_ID);
        spy = client.addSubscription(spyForChannel(channel), STREAM_ID);
        subscription = client.addSubscription(channel, STREAM_ID);

        while (!spy.isConnected() || !subscription.isConnected() || !publication.isConnected())
        {
            Thread.yield();
        }

        while (fragmentsReadFromSpy < messagesToSend)
        {
            if (messagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    messagesLeftToSend--;
                }
            }

            fragmentsReadFromSpy += spy.poll(fragmentHandlerSpy, 10);

            // subscription receives up to 1/8 of the messages, then stops
            if (fragmentsReadFromSubscription < (messagesToSend / 8))
            {
                fragmentsReadFromSubscription += subscription.poll(fragmentHandlerSubscription, 10);
            }
            else if (!isSubscriptionClosed)
            {
                subscription.close();
                isSubscriptionClosed = true;
            }
        }

        verify(fragmentHandlerSpy, times(messagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }
}
