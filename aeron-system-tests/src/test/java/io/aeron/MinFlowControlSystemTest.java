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

import io.aeron.driver.FlowControlSupplier;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SenderLimit;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.aeron.AeronCounters.FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID;
import static io.aeron.FlowControlTests.awaitConnectionAndStatusMessages;
import static io.aeron.test.Tests.awaitConnected;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class MinFlowControlSystemTest
{
    private static final String MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";
    private static final int STREAM_ID = 1001;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private final MediaDriver.Context driverAContext = new MediaDriver.Context();
    private final MediaDriver.Context driverBContext = new MediaDriver.Context();

    @TempDir
    private Path tempDir;

    private Aeron clientA;
    private Aeron clientB;
    private TestMediaDriver driverA;
    private TestMediaDriver driverB;
    private Publication publication;
    private Subscription subscriptionA;
    private Subscription subscriptionB;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private final FragmentHandler fragmentHandlerA = mock(FragmentHandler.class);
    private final FragmentHandler fragmentHandlerB = mock(FragmentHandler.class);

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private void launch()
    {
        buffer.putInt(0, 1);

        final String baseDirA = tempDir.resolve("A").toString();
        final String baseDirB = tempDir.resolve("B").toString();

        driverAContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .flowControlReceiverTimeoutNs(TimeUnit.MILLISECONDS.toNanos(1000))
            .errorHandler(Tests::onError)
            .threadingMode(ThreadingMode.SHARED);

        driverBContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .flowControlReceiverTimeoutNs(TimeUnit.MILLISECONDS.toNanos(1000))
            .errorHandler(Tests::onError)
            .threadingMode(ThreadingMode.SHARED);

        driverA = TestMediaDriver.launch(driverAContext, testWatcher);
        testWatcher.dataCollector().add(driverAContext.aeronDirectory());

        driverB = TestMediaDriver.launch(driverBContext, testWatcher);
        testWatcher.dataCollector().add(driverBContext.aeronDirectory());

        clientA = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverAContext.aeronDirectoryName()));

        clientB = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverBContext.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.quietCloseAll(clientB, clientA, driverB, driverA);
    }

    private static Stream<Arguments> strategyConfigurations()
    {
        return Stream.of(
            Arguments.of(new MinMulticastFlowControlSupplier(), ""),
            Arguments.of(null, "|fc=min"),
            Arguments.of(null, "|fc=min,g:/1"));
    }

    @ParameterizedTest
    @MethodSource("strategyConfigurations")
    @InterruptAfter(10)
    void shouldSlowToMinMulticastFlowControlStrategy(
        final FlowControlSupplier flowControlSupplier, final String publisherUriParams)
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int numMessagesLeftToSend = numMessagesToSend;
        int numFragmentsFromB = 0;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        if (null != flowControlSupplier)
        {
            driverAContext.multicastFlowControlSupplier(flowControlSupplier);
        }
        driverAContext.flowControlGroupMinSize(1);

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI + publisherUriParams, STREAM_ID);

        final int flowControlCounterId = clientA.countersReader().findByTypeIdAndRegistrationId(
            FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID, publication.registrationId());
        assertNotEquals(NULL_COUNTER_ID, flowControlCounterId);

        while (!subscriptionA.isConnected() ||
            !subscriptionB.isConnected() ||
            !publication.isConnected() ||
            clientA.countersReader().getCounterValue(flowControlCounterId) < 2)
        {
            Tests.yield();
        }

        for (long i = 0; numFragmentsFromB < numMessagesToSend; i++)
        {
            if (numMessagesLeftToSend > 0)
            {
                final long result = publication.offer(buffer, 0, buffer.capacity());
                if (result >= 0L)
                {
                    numMessagesLeftToSend--;
                }
                else if (Publication.NOT_CONNECTED == result)
                {
                    fail("Publication not connected, numMessagesLeftToSend=" + numMessagesLeftToSend);
                }
            }

            Tests.yield();

            // A keeps up
            subscriptionA.poll(fragmentHandlerA, 10);

            // B receives slowly
            if ((i % 2) == 0)
            {
                final int bFragments = subscriptionB.poll(fragmentHandlerB, 1);
                if (0 == bFragments && !subscriptionB.isConnected())
                {
                    if (subscriptionB.isClosed())
                    {
                        fail("Subscription B is closed, numFragmentsFromB=" + numFragmentsFromB);
                    }

                    fail("Subscription B not connected, numFragmentsFromB=" + numFragmentsFromB);
                }

                numFragmentsFromB += bFragments;
            }
        }

        verify(fragmentHandlerB, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class), anyInt(), eq(MESSAGE_LENGTH), any(Header.class));
    }

    @Test
    @InterruptAfter(10)
    void shouldRemoveDeadReceiverWithMinMulticastFlowControlStrategy()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        final MutableInteger numMessagesLeftToSend = new MutableInteger(numMessagesToSend);
        final MutableInteger numFragmentsReadFromA = new MutableInteger(0);
        final MutableInteger numFragmentsReadFromB = new MutableInteger(0);

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driverAContext.multicastFlowControlSupplier(new MinMulticastFlowControlSupplier());

        launch();

        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        awaitConnected(subscriptionA);

        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        awaitConnected(subscriptionB);

        awaitConnected(publication);

        boolean isBClosed = false;
        while (numFragmentsReadFromA.get() < numMessagesToSend)
        {
            int workDone = 0;
            if (numMessagesLeftToSend.get() > 0)
            {
                final long position = publication.offer(buffer, 0, buffer.capacity());
                if (position >= 0L)
                {
                    numMessagesLeftToSend.decrement();
                    workDone++;
                }
            }

            // A keeps up
            final int readA = subscriptionA.poll(fragmentHandlerA, 10);
            numFragmentsReadFromA.addAndGet(readA);
            workDone += readA;

            // B receives up to 1/8 of the messages, then stops
            if (numFragmentsReadFromB.get() < (numMessagesToSend / 8))
            {
                final int readB = subscriptionB.poll(fragmentHandlerB, 10);
                numFragmentsReadFromB.addAndGet(readB);
                workDone += readB;
            }
            else if (!isBClosed)
            {
                subscriptionB.close();
                isBClosed = true;
            }

            if (0 == workDone)
            {
                Tests.yieldingIdle(
                    () -> "numMessagesToSend=" + numMessagesToSend + " numMessagesLeftToSend=" + numMessagesLeftToSend +
                    " numFragmentsReadFromA=" + numFragmentsReadFromA + " numFragmentsReadFromB=" +
                    numFragmentsReadFromB);
            }
        }

        verify(fragmentHandlerA, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class), anyInt(), eq(MESSAGE_LENGTH), any(Header.class));
    }

    @SlowTest
    @Test
    @InterruptAfter(20)
    void shouldPreventConnectionUntilGroupMinSizeIsMet()
    {
        final Integer groupSize = 3;

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("224.20.30.39:24326")
            .networkInterface("localhost");

        final String uriPlain = builder
            .flowControl((String)null)
            .build();

        final String uriWithMinFlowControl = builder
            .groupTag((Long)null)
            .minFlowControl(groupSize, null)
            .build();

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        launch();

        final CountersReader countersReader = clientA.countersReader();
        TestMediaDriver driverC = null;
        Aeron clientC = null;
        Publication publication = null;
        Subscription subscription0 = null;
        Subscription subscription1 = null;
        Subscription subscription2 = null;

        try
        {
            driverC = TestMediaDriver.launch(
                new MediaDriver.Context().publicationTermBufferLength(TERM_BUFFER_LENGTH)
                    .aeronDirectoryName(tempDir.resolve("C").toString())
                    .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                    .errorHandler(Tests::onError)
                    .threadingMode(ThreadingMode.SHARED),
                testWatcher);

            clientC = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverC.aeronDirectoryName()));

            subscription0 = clientA.addSubscription(uriPlain, STREAM_ID);
            subscription1 = clientB.addSubscription(uriPlain, STREAM_ID);
            publication = clientA.addPublication(uriWithMinFlowControl, STREAM_ID);

            awaitConnectionAndStatusMessages(countersReader, subscription0, subscription1);

            assertFalse(publication.isConnected());

            subscription2 = clientC.addSubscription(uriPlain, STREAM_ID);

            // Should now have 3 receivers and publication should eventually be connected.
            while (!publication.isConnected())
            {
                Tests.sleep(1);
            }

            subscription2.close();
            subscription2 = null;

            // Lost a receiver and publication should eventually be disconnected.
            while (publication.isConnected())
            {
                Tests.sleep(1);
            }

            subscription2 = clientC.addSubscription(uriPlain, STREAM_ID);

            while (!publication.isConnected())
            {
                Tests.sleep(1);
            }
        }
        finally
        {
            CloseHelper.closeAll(
                publication,
                subscription0, subscription1, subscription2,
                clientC,
                driverC);
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldPreventConnectionUntilAtLeastOneSubscriberConnectedWithRequiredGroupSizeZero()
    {
        final Integer groupSize = 0;

        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("224.20.30.41:24326")
            .networkInterface("localhost");

        final String plainUri = builder
            .flowControl((String)null)
            .build();

        final String uriWithMinFlowControl = builder
            .groupTag((Long)null)
            .minFlowControl(groupSize, null)
            .build();

        launch();

        clientA.addSubscription(plainUri, STREAM_ID + 1);
        publication = clientA.addPublication(uriWithMinFlowControl, STREAM_ID);
        final Publication otherPublication = clientA.addPublication(plainUri, STREAM_ID + 1);

        awaitConnected(otherPublication);

        // We know another publication on the same channel is connected

        subscriptionA = clientA.addSubscription(plainUri, STREAM_ID);

        while (!publication.isConnected())
        {
            Tests.sleep(1);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldHandleSenderLimitCorrectlyWithMinGroupSize()
    {
        final String publisherUri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:123/1";
        final String groupSubscriberUri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|gtag=123";
        final String subscriberUri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));

        launch();

        subscriptionA = clientA.addSubscription(subscriberUri, STREAM_ID);
        publication = clientA.addPublication(publisherUri, STREAM_ID);

        final CountersReader countersReader = clientA.countersReader();

        final int senderLimitCounterId = FlowControlTests.findCounterIdByRegistrationId(
            countersReader, SenderLimit.SENDER_LIMIT_TYPE_ID, publication.registrationId);
        final long currentSenderLimit = countersReader.getCounterValue(senderLimitCounterId);

        awaitConnectionAndStatusMessages(countersReader, subscriptionA);
        assertEquals(currentSenderLimit, countersReader.getCounterValue(senderLimitCounterId));

        subscriptionB = clientB.addSubscription(groupSubscriberUri, STREAM_ID);

        while (currentSenderLimit == countersReader.getCounterValue(senderLimitCounterId))
        {
            Tests.sleep(
                1,
                "currentSenderLimit(%d) == countersReader.getCounterValue(senderLimitCounterId)(%d)",
                currentSenderLimit,
                countersReader.getCounterValue(senderLimitCounterId));
        }
    }
}
