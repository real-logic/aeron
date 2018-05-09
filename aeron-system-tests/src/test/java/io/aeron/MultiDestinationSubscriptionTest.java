/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class MultiDestinationSubscriptionTest
{
    private static final String PUB_UNICAST_URI = "aeron:udp?endpoint=localhost:54325";
    private static final String PUB_MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost";
    private static final String PUB_MDC_URI = "aeron:udp?control=localhost:54325|control-mode=dynamic";

    private static final String SUB_URI = "aeron:udp?control-mode=manual";
    private static final String SUB_MDC_DESTINATION_URI = "aeron:udp?endpoint=localhost:54326|control=localhost:54325";

    private static final int STREAM_ID = 1;

    private static final int TERM_BUFFER_LENGTH = 64 * 1024;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;

    private final MediaDriver.Context driverContext = new MediaDriver.Context();

    private Aeron clientA;
    private Aeron clientB;
    private MediaDriver driver;
    private Publication publication;
    private Subscription subscription;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private FragmentHandler fragmentHandler = mock(FragmentHandler.class);

    private void launch()
    {
        buffer.putInt(0, 1);

        driverContext
            .errorHandler(Throwable::printStackTrace)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .threadingMode(ThreadingMode.SHARED);

        driver = MediaDriver.launch(driverContext);
        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverContext.aeronDirectoryName()));
        clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverContext.aeronDirectoryName()));
    }

    @After
    public void closeEverything()
    {
        CloseHelper.close(publication);
        CloseHelper.close(subscription);
        CloseHelper.close(clientB);
        CloseHelper.close(clientA);
        CloseHelper.close(driver);

        driverContext.deleteAeronDirectory();
    }

    @Test(timeout = 10_000)
    public void shouldSpinUpAndShutdownWithUnicast()
    {
        launch();

        publication = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);
        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);

        subscription.addDestination(PUB_UNICAST_URI);

        while (subscription.hasNoImages())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Test(timeout = 10_000)
    public void shouldSpinUpAndShutdownWithMulticast()
    {
        launch();

        publication = clientA.addPublication(PUB_MULTICAST_URI, STREAM_ID);
        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);

        subscription.addDestination(PUB_MULTICAST_URI);

        while (subscription.hasNoImages())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Test(timeout = 10_000)
    public void shouldSpinUpAndShutdownWithDynamicMdc()
    {
        launch();

        publication = clientA.addPublication(PUB_MDC_URI, STREAM_ID);
        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);

        subscription.addDestination(SUB_MDC_DESTINATION_URI);

        while (subscription.hasNoImages())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Test(timeout = 10_000)
    public void shouldSendToSingleDestinationSubscriptionWithUnicast()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch();

        publication = clientA.addPublication(PUB_UNICAST_URI, STREAM_ID);
        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);

        subscription.addDestination(PUB_UNICAST_URI);

        while (subscription.hasNoImages())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();
            pollForFragment(subscription, fragmentHandler, fragmentsRead);
        }

        verifyFragments(fragmentHandler, numMessagesToSend);
    }

    @Test(timeout = 10_000)
    public void shouldSendToSingleDestinationSubscriptionWithMulticast()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch();

        publication = clientA.addPublication(PUB_MULTICAST_URI, STREAM_ID);
        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);

        subscription.addDestination(PUB_MULTICAST_URI);

        while (subscription.hasNoImages())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();
            pollForFragment(subscription, fragmentHandler, fragmentsRead);
        }

        verifyFragments(fragmentHandler, numMessagesToSend);
    }

    @Test(timeout = 10_000)
    public void shouldSendToSingleDestinationSubscriptionWithDynamicMdc()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch();

        publication = clientA.addPublication(PUB_MDC_URI, STREAM_ID);
        subscription = clientA.addSubscription(SUB_URI, STREAM_ID);

        subscription.addDestination(SUB_MDC_DESTINATION_URI);

        while (subscription.hasNoImages())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();
            pollForFragment(subscription, fragmentHandler, fragmentsRead);
        }

        verifyFragments(fragmentHandler, numMessagesToSend);
    }

    private void pollForFragment(
        final Subscription subscription, final FragmentHandler handler, final MutableInteger fragmentsRead)
    {
        SystemTest.executeUntil(
            () -> fragmentsRead.get() > 0,
            (j) ->
            {
                fragmentsRead.value += subscription.poll(handler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));
    }

    private void verifyFragments(final FragmentHandler fragmentHandler, final int numMessagesToSend)
    {
        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }
}
