/*
 * Copyright 2014-2025 Real Logic Limited.
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
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class MultiDestinationCastTest
{
    private static final String PUB_MDC_DYNAMIC_URI = "aeron:udp?control=localhost:24325|control-mode=dynamic|fc=min";
    private static final String SUB1_MDC_DYNAMIC_URI = "aeron:udp?control=localhost:24325|group=true";
    private static final String SUB2_MDC_DYNAMIC_URI = "aeron:udp?control=localhost:24325|group=true";
    private static final String SUB3_MDC_DYNAMIC_URI = CommonContext.SPY_PREFIX + PUB_MDC_DYNAMIC_URI;

    private static final String PUB_MDC_MANUAL_URI = "aeron:udp?control-mode=manual";
    private static final String SUB1_MDC_MANUAL_URI = "aeron:udp?endpoint=localhost:24326|group=true";
    private static final String SUB2_MDC_MANUAL_URI = "aeron:udp?endpoint=localhost:24327|group=true";

    private static final int STREAM_ID = 1001;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR = CommonContext.getAeronDirectoryName() + File.separator;
    private static final int FRAGMENT_LIMIT = 10;

    private final MediaDriver.Context driverBContext = new MediaDriver.Context();
    private final MediaDriver.Context driverAContext = new MediaDriver.Context();

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
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private void launch(final ErrorHandler errorHandler)
    {
        final String baseDirA = ROOT_DIR + "A";
        final String baseDirB = ROOT_DIR + "B";

        buffer.putInt(0, 1);

        driverAContext.errorHandler(errorHandler)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .threadingMode(ThreadingMode.SHARED);

        driverBContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .errorHandler(errorHandler)
            .aeronDirectoryName(baseDirB)
            .threadingMode(ThreadingMode.SHARED);

        driverA = TestMediaDriver.launch(driverAContext, testWatcher);
        testWatcher.dataCollector().add(driverA.context().aeronDirectory());
        driverB = TestMediaDriver.launch(driverBContext, testWatcher);
        testWatcher.dataCollector().add(driverB.context().aeronDirectory());
        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverAContext.aeronDirectoryName()));
        clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverBContext.aeronDirectoryName()));
    }

    @AfterEach
    void closeEverything()
    {
        CloseHelper.closeAll(clientB, clientA, driverB, driverA);
    }

    @Test
    @InterruptAfter(10)
    void shouldSpinUpAndShutdownWithDynamic()
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
    @InterruptAfter(10)
    @SlowTest
    void shouldSpinUpAndShutdownWithManual()
    {
        driverAContext.imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1));
        driverBContext.imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1));

        launch(Tests::onError);

        final String taggedMdcUri = new ChannelUriStringBuilder(PUB_MDC_MANUAL_URI).tags(
            clientA.nextCorrelationId(),
            clientA.nextCorrelationId()).build();
        final String spySubscriptionUri = new ChannelUriStringBuilder(taggedMdcUri).prefix("aeron-spy").build();

        subscriptionA = clientA.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(SUB2_MDC_MANUAL_URI, STREAM_ID);
        subscriptionC = clientA.addSubscription(spySubscriptionUri, STREAM_ID);

        publication = clientA.addPublication(taggedMdcUri, STREAM_ID);
        publication.addDestination(SUB1_MDC_MANUAL_URI);
        final long correlationId = publication.asyncAddDestination(SUB2_MDC_MANUAL_URI);

        while (subscriptionA.hasNoImages() || subscriptionB.hasNoImages() || subscriptionC.hasNoImages())
        {
            Tests.yield();
        }

        assertFalse(clientA.isCommandActive(correlationId));

        final long removeCorrelationId = publication.asyncRemoveDestination(SUB2_MDC_MANUAL_URI);
        Tests.await("Remove Active", () -> !clientA.isCommandActive(removeCorrelationId));
        Tests.await("Subscription disconnect", () -> subscriptionB.hasNoImages());
    }

    @Test
    @InterruptAfter(20)
    void shouldSendToTwoPortsWithDynamic()
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
    @InterruptAfter(20)
    void shouldSendToTwoPortsWithDynamicSingleDriver()
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
    @InterruptAfter(10)
    void shouldSendToTwoPortsWithManualSingleDriver()
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
    @InterruptAfter(10)
    void addDestinationWithSpySubscriptionsShouldFailWithRegistrationException()
    {
        testWatcher.ignoreErrorsMatching(s -> s.contains("spies are invalid"));
        final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);
        launch(mockErrorHandler);

        publication = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        final RegistrationException registrationException = assertThrows(
            RegistrationException.class,
            () -> publication.addDestination(CommonContext.SPY_PREFIX + PUB_MDC_DYNAMIC_URI));

        assertThat(registrationException.getMessage(), containsString("spies are invalid"));
    }

    @Test
    @InterruptAfter(10)
    void shouldManuallyRemovePortDuringActiveStream() throws InterruptedException
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
                if (0 == subscriptionB.poll(fragmentHandlerB, FRAGMENT_LIMIT))
                {
                    Tests.yield();
                }
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
    @InterruptAfter(10)
    void shouldManuallyAddPortDuringActiveStream() throws InterruptedException
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
                Tests.yieldingIdle(positionSupplier);
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
                        Tests.yieldingIdle(messageSupplierA);
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
                Tests.yieldingIdle(messageSupplierA);
            }

            if (fragmentHandlerB.notDone(numMessageForSub2) &&
                subscriptionB.poll(fragmentHandlerB, FRAGMENT_LIMIT) <= 0)
            {
                Tests.yieldingIdle(messageSupplierB);
            }
        }
    }

    @Test
    void shouldSpyOnDynamicMdcConnectionWithWildcardPortUsingTags()
    {
        launch(Tests::onError);

        final long pubTag = clientA.nextCorrelationId();

        final String uri = new ChannelUriStringBuilder("aeron:udp?control=localhost:0|control-mode=dynamic|ssc=true")
            .tags(pubTag, null)
            .build();

        final String spyUri = new ChannelUriStringBuilder().prefix(SPY_QUALIFIER)
            .media("udp")
            .tags(pubTag, null)
            .build();

        publication = clientA.addPublication(uri, STREAM_ID);
        subscriptionA = clientA.addSubscription(spyUri, STREAM_ID);

        Tests.awaitConnected(publication);

        while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
        {
            Tests.yield();
        }

        pollForFragment(subscriptionA, fragmentHandlerA);
    }

    @Test
    void shouldHandleSubscriptionsIfUsingTagsAndEndpointsMatchButSessionIdInUse()
    {
        launch(Tests::onError);

        try (
            Publication pub1 = clientA.addExclusivePublication(PUB_MDC_DYNAMIC_URI, STREAM_ID);
            Publication pub2 = clientA.addExclusivePublication(PUB_MDC_DYNAMIC_URI, STREAM_ID))
        {

            final String sub1Uri = new ChannelUriStringBuilder(SUB1_MDC_DYNAMIC_URI)
                .endpoint("127.0.0.1:0")
                .tags("1001")
                .sessionId(pub1.sessionId())
                .build();

            final String sub2Uri = new ChannelUriStringBuilder(SUB1_MDC_DYNAMIC_URI)
                .endpoint("127.0.0.1:0")
                .tags("1001")
                .sessionId(pub2.sessionId())
                .build();

            try (
                Subscription sub1 = clientB.addSubscription(sub1Uri, STREAM_ID);
                Subscription sub2 = clientB.addSubscription(sub2Uri, STREAM_ID))
            {
                Tests.awaitConnected(sub1);
                Tests.awaitConnected(sub2);
                Tests.awaitConnected(pub1);
                Tests.awaitConnected(pub2);

                while (pub1.offer(buffer, 0, buffer.capacity()) < 0L)
                {
                    Tests.yield();
                }

                while (pub2.offer(buffer, 0, buffer.capacity()) < 0L)
                {
                    Tests.yield();
                }

                final long nowMs = System.currentTimeMillis();
                long sub1Count = 0;
                long sub2Count = 0;
                while (System.currentTimeMillis() - nowMs < 500L)
                {
                    sub1Count += sub1.poll((a, b, c, d) -> {}, 10);
                    sub2Count += sub2.poll((a, b, c, d) -> {}, 10);

                    assertThat(sub1Count, lessThan(2L));
                    assertThat(sub2Count, lessThan(2L));
                }
            }
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(5)
    void shouldNotAllowMdcSubscriptionsWhenChannelHasControlButNotSpecifiedAsMdc()
    {
        launch(Tests::onError);

        try (
            Publication pub = clientA.addPublication(
                "aeron:udp?control=localhost:24325|endpoint=localhost:10000", STREAM_ID);
            Subscription subOk = clientA.addSubscription(
                "aeron:udp?control=localhost:24325|endpoint=localhost:10000", STREAM_ID);
            Subscription subWrong = clientA.addSubscription(
                "aeron:udp?control=localhost:24325|endpoint=localhost:10001", STREAM_ID))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(subOk);

            final long deadlineMs = System.currentTimeMillis() + 2_000;
            while (System.currentTimeMillis() < deadlineMs)
            {
                assertFalse(subWrong.isConnected());
                Tests.sleep(1);
            }
        }
    }

    @Test
    @InterruptAfter(5)
    @SlowTest
    void shouldAllowMdcDestinationEndpointsToBeShared()
    {
        launch(Tests::onError);

        try (
            Publication mdcA = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
            Publication mdcB = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID + 1);
            Subscription subA = clientB.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
            Subscription subB = clientB.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID + 1))
        {
            mdcA.addDestination(SUB1_MDC_MANUAL_URI);
            mdcB.addDestination(SUB1_MDC_MANUAL_URI);
            Tests.awaitConnected(mdcA);
            Tests.awaitConnected(mdcB);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);

            while (mdcA.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subA, fragmentHandlerA);
            assertNoFragmentsReceived(subB, 1_000L);

            while (mdcB.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(subB, fragmentHandlerA);
            assertNoFragmentsReceived(subA, 1_000L);
        }
    }

    @Test
    @InterruptAfter(20)
    @SlowTest
    void shouldRemoveDestinationUsingRegistrationId()
    {
        driverAContext.imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1));
        driverBContext.imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1));

        launch(Tests::onError);

        try (
            Publication mdc = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
            Subscription sub1 = clientB.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
            Subscription sub2 = clientB.addSubscription(SUB2_MDC_MANUAL_URI, STREAM_ID))
        {
            final long registrationId1 = mdc.asyncAddDestination(SUB1_MDC_MANUAL_URI);
            final long registrationId2 = mdc.asyncAddDestination(SUB2_MDC_MANUAL_URI);
            while (clientA.isCommandActive(registrationId2))
            {
                Tests.yield();
            }

            Tests.await("Connections", mdc::isConnected, sub1::isConnected, sub2::isConnected);

            while (mdc.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(sub1, fragmentHandlerA);
            pollForFragment(sub2, fragmentHandlerB);

            mdc.removeDestination(registrationId2);

            Tests.await("Disconnected", () -> !sub2.isConnected());

            while (mdc.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            pollForFragment(sub1, fragmentHandlerA);
            assertNoFragmentsReceived(sub2, 1_000L);

            final long correlationId = mdc.asyncRemoveDestination(registrationId1);
            Tests.await("Command Active", () -> !clientA.isCommandActive(correlationId));
            Tests.await("Disconnected", () -> !sub1.isConnected(), () -> !mdc.isConnected());
        }
    }

    private static void assertNoFragmentsReceived(final Subscription subB, final long noMessageTimeout)
    {
        final long t0 = System.currentTimeMillis();
        while (System.currentTimeMillis() - t0 < noMessageTimeout)
        {
            assertEquals(0, subB.poll((a, b, c, d) -> {}, 10));
            Tests.yield();
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
        while (totalFragments < 1 && ((nowNs - startNs) < TimeUnit.SECONDS.toNanos(10)));
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
