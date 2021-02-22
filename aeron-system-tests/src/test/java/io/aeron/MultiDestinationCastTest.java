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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.CountingFragmentHandler;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.*;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class MultiDestinationCastTest
{
    private static final String PUB_MDC_DYNAMIC_URI = "aeron:udp?control=localhost:24325";
    private static final String SUB1_MDC_DYNAMIC_URI = "aeron:udp?control=localhost:24325|group=true";
    private static final String SUB2_MDC_DYNAMIC_URI = "aeron:udp?control=localhost:24325|group=true";
    private static final String SUB3_MDC_DYNAMIC_URI = CommonContext.SPY_PREFIX + PUB_MDC_DYNAMIC_URI;

    private static final String PUB_MDC_MANUAL_URI = "aeron:udp?control-mode=manual|tags=3,4";
    private static final String SUB1_MDC_MANUAL_URI = "aeron:udp?endpoint=localhost:24326|group=true";
    private static final String SUB2_MDC_MANUAL_URI = "aeron:udp?endpoint=localhost:24327|group=true";
    private static final String SUB3_MDC_MANUAL_URI = CommonContext.SPY_PREFIX + PUB_MDC_MANUAL_URI;

    private static final int STREAM_ID = 1001;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR = CommonContext.getAeronDirectoryName() + File.separator;
    private static final int FRAGMENT_LIMIT = 10;

    private final MediaDriver.Context driverBContext = new MediaDriver.Context();

    private Aeron clientA;
    private Aeron clientB;
    private TestMediaDriver driverA;
    private TestMediaDriver driverB;
    private Publication publication;
    private Subscription subscriptionA;
    private Subscription subscriptionB;
    private Subscription subscriptionC;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private final FragmentHandler fragmentHandlerA = mock(FragmentHandler.class, "fragmentHandlerA");
    private final FragmentHandler fragmentHandlerB = mock(FragmentHandler.class, "fragmentHandlerB");
    private final FragmentHandler fragmentHandlerC = mock(FragmentHandler.class, "fragmentHandlerC");

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private void launch(final ErrorHandler errorHandler)
    {
        final String baseDirA = ROOT_DIR + "A";
        final String baseDirB = ROOT_DIR + "B";

        buffer.putInt(0, 1);

        final MediaDriver.Context driverAContext = new MediaDriver.Context()
            .errorHandler(errorHandler)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .threadingMode(ThreadingMode.SHARED);

        driverBContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .errorHandler(errorHandler)
            .aeronDirectoryName(baseDirB)
            .threadingMode(ThreadingMode.SHARED);

        driverA = TestMediaDriver.launch(driverAContext, testWatcher);
        driverB = TestMediaDriver.launch(driverBContext, testWatcher);
        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverAContext.aeronDirectoryName()));
        clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverBContext.aeronDirectoryName()));
    }

    @AfterEach
    public void closeEverything()
    {
        CloseHelper.closeAll(clientB, clientA, driverB, driverA);
        IoUtil.delete(new File(ROOT_DIR), true);
    }

    @Test
    @Timeout(10)
    public void shouldSpinUpAndShutdownWithDynamic()
    {
        launch(Tests::onError);

        publication = clientA.addPublication(PUB_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionA = clientA.addSubscription(SUB1_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(SUB2_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionC = clientA.addSubscription(SUB3_MDC_DYNAMIC_URI, STREAM_ID);

        while (subscriptionA.hasNoImages() || subscriptionB.hasNoImages() || subscriptionC.hasNoImages())
        {
            Tests.yield();
        }
    }

    @Test
    @Timeout(10)
    public void shouldSpinUpAndShutdownWithManual()
    {
        launch(Tests::onError);

        subscriptionA = clientA.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(SUB2_MDC_MANUAL_URI, STREAM_ID);
        subscriptionC = clientA.addSubscription(SUB3_MDC_MANUAL_URI, STREAM_ID);

        publication = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        publication.addDestination(SUB1_MDC_MANUAL_URI);
        final long correlationId = publication.asyncAddDestination(SUB2_MDC_MANUAL_URI);

        while (subscriptionA.hasNoImages() || subscriptionB.hasNoImages() || subscriptionC.hasNoImages())
        {
            Tests.yield();
        }

        assertFalse(clientA.isCommandActive(correlationId));
    }

    @Test
    @Timeout(20)
    public void shouldSendToTwoPortsWithDynamic()
    {
        final int numMessagesToSend = MESSAGES_PER_TERM * 3;

        launch(Tests::onError);

        subscriptionA = clientA.addSubscription(SUB1_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(SUB2_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionC = clientA.addSubscription(SUB3_MDC_DYNAMIC_URI, STREAM_ID);
        publication = clientA.addPublication(PUB_MDC_DYNAMIC_URI, STREAM_ID);

        while (subscriptionA.hasNoImages() || subscriptionB.hasNoImages() || subscriptionC.hasNoImages())
        {
            Tests.yield();
        }

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscriptionA, fragmentHandlerA);
            pollForFragment(subscriptionB, fragmentHandlerB);
            pollForFragment(subscriptionC, fragmentHandlerC);
        }

        verifyFragments(fragmentHandlerA, numMessagesToSend);
        verifyFragments(fragmentHandlerB, numMessagesToSend);
        verifyFragments(fragmentHandlerC, numMessagesToSend);
    }

    @Test
    @Timeout(20)
    public void shouldSendToTwoPortsWithDynamicSingleDriver()
    {
        final int numMessagesToSend = MESSAGES_PER_TERM * 3;

        launch(Tests::onError);

        subscriptionA = clientA.addSubscription(SUB1_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionB = clientA.addSubscription(SUB2_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionC = clientA.addSubscription(SUB3_MDC_DYNAMIC_URI, STREAM_ID);
        publication = clientA.addPublication(PUB_MDC_DYNAMIC_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected() || !subscriptionC.isConnected())
        {
            Tests.yield();
        }

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscriptionA, fragmentHandlerA);
            pollForFragment(subscriptionB, fragmentHandlerB);
            pollForFragment(subscriptionC, fragmentHandlerC);
        }

        verifyFragments(fragmentHandlerA, numMessagesToSend);
        verifyFragments(fragmentHandlerB, numMessagesToSend);
        verifyFragments(fragmentHandlerC, numMessagesToSend);
    }

    @Test
    @Timeout(10)
    public void shouldSendToTwoPortsWithManualSingleDriver()
    {
        final int numMessagesToSend = MESSAGES_PER_TERM * 3;

        launch(Tests::onError);

        subscriptionA = clientA.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
        subscriptionB = clientA.addSubscription(SUB2_MDC_MANUAL_URI, STREAM_ID);

        publication = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        publication.addDestination(SUB1_MDC_MANUAL_URI);
        publication.addDestination(SUB2_MDC_MANUAL_URI);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
        {
            Tests.yield();
        }

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscriptionA, fragmentHandlerA);
            pollForFragment(subscriptionB, fragmentHandlerB);
        }

        verifyFragments(fragmentHandlerA, numMessagesToSend);
        verifyFragments(fragmentHandlerB, numMessagesToSend);
    }

    @Test
    @Timeout(10)
    public void addDestinationWithSpySubscriptionsShouldFailWithRegistrationException()
    {
        final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);
        launch(mockErrorHandler);

        publication = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        final RegistrationException registrationException = assertThrows(
            RegistrationException.class,
            () -> publication.addDestination(CommonContext.SPY_PREFIX + PUB_MDC_DYNAMIC_URI));

        assertThat(registrationException.getMessage(), containsString("spies are invalid"));
    }

    @Test
    @Timeout(10)
    public void shouldManuallyRemovePortDuringActiveStream() throws InterruptedException
    {
        final int numMessagesToSend = MESSAGES_PER_TERM * 3;
        final int numMessageForSub2 = 10;
        final CountDownLatch unavailableImage = new CountDownLatch(1);

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        launch(Tests::onError);

        subscriptionA = clientA.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(
            SUB2_MDC_MANUAL_URI, STREAM_ID, null, (image) -> unavailableImage.countDown());

        publication = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        publication.addDestination(SUB1_MDC_MANUAL_URI);
        publication.addDestination(SUB2_MDC_MANUAL_URI);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
        {
            Tests.yield();
        }

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, MESSAGE_LENGTH) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subscriptionA, fragmentHandlerA);

            if (i < numMessageForSub2)
            {
                pollForFragment(subscriptionB, fragmentHandlerB);
            }
            else
            {
                subscriptionB.poll(fragmentHandlerB, FRAGMENT_LIMIT);
                Tests.yield();
            }

            if (i == numMessageForSub2 - 1)
            {
                publication.removeDestination(SUB2_MDC_MANUAL_URI);
            }
        }

        unavailableImage.await();

        verifyFragments(fragmentHandlerA, numMessagesToSend);
        verifyFragments(fragmentHandlerB, numMessageForSub2);
    }

    @Test
    @Timeout(10)
    public void shouldManuallyAddPortDuringActiveStream() throws InterruptedException
    {
        final int numMessagesToSend = MESSAGES_PER_TERM * 3;
        final int numMessageForSub2 = 10;
        final CountingFragmentHandler fragmentHandlerA = new CountingFragmentHandler("fragmentHandlerA");
        final CountingFragmentHandler fragmentHandlerB = new CountingFragmentHandler("fragmentHandlerB");
        final Supplier<String> messageSupplierA = fragmentHandlerA::toString;
        final Supplier<String> messageSupplierB = fragmentHandlerB::toString;
        final CountDownLatch availableImage = new CountDownLatch(1);
        final MutableLong position = new MutableLong(0);
        final MutableInteger messagesSent = new MutableInteger(0);
        final Supplier<String> positionSupplier =
            () -> "Failed to publish, position: " + position + ", sent: " + messagesSent;

        launch(Tests::onError);

        subscriptionA = clientA.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(
            SUB2_MDC_MANUAL_URI, STREAM_ID, (image) -> availableImage.countDown(), null);

        publication = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        publication.addDestination(SUB1_MDC_MANUAL_URI);

        Tests.awaitConnected(subscriptionA);

        while (messagesSent.value < numMessagesToSend)
        {
            position.value = publication.offer(buffer, 0, MESSAGE_LENGTH);

            if (0 <= position.value)
            {
                messagesSent.increment();
            }
            else
            {
                Tests.yieldingWait(positionSupplier);
            }

            subscriptionA.poll(fragmentHandlerA, FRAGMENT_LIMIT);

            if (messagesSent.value > (numMessagesToSend - numMessageForSub2))
            {
                subscriptionB.poll(fragmentHandlerB, FRAGMENT_LIMIT);
            }

            if (messagesSent.value == (numMessagesToSend - numMessageForSub2))
            {
                final int published = messagesSent.value;
                // If we add B before A has reached `published` number of messages
                // then B will receive more than the expected `numMessageForSub2`.
                while (fragmentHandlerA.notDone(published))
                {
                    if (subscriptionA.poll(fragmentHandlerA, FRAGMENT_LIMIT) <= 0)
                    {
                        Tests.yieldingWait(messageSupplierA);
                    }
                }

                publication.addDestination(SUB2_MDC_MANUAL_URI);
                availableImage.await();
            }
        }

        while (fragmentHandlerA.notDone(numMessagesToSend) || fragmentHandlerB.notDone(numMessageForSub2))
        {
            if (fragmentHandlerA.notDone(numMessagesToSend) &&
                subscriptionA.poll(fragmentHandlerA, FRAGMENT_LIMIT) <= 0)
            {
                Tests.yieldingWait(messageSupplierA);
            }

            if (fragmentHandlerB.notDone(numMessageForSub2) &&
                subscriptionB.poll(fragmentHandlerB, FRAGMENT_LIMIT) <= 0)
            {
                Tests.yieldingWait(messageSupplierB);
            }
        }
    }

    private static void pollForFragment(final Subscription subscription, final FragmentHandler handler)
    {
        final long startNs = System.nanoTime();
        long nowNs = startNs;
        int totalFragments = 0;

        do
        {
            final int numFragments = subscription.poll(handler, FRAGMENT_LIMIT);
            if (numFragments <= 0)
            {
                Thread.yield();
                Tests.checkInterruptStatus();
                nowNs = System.nanoTime();
            }
            else
            {
                totalFragments += numFragments;
            }
        }
        while (totalFragments < 1 && ((nowNs - startNs) < TimeUnit.MILLISECONDS.toNanos(5000)));
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
