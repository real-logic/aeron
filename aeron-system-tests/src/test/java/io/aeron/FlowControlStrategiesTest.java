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

import io.aeron.driver.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

/**
 * Tests requiring multiple embedded drivers for FlowControl strategies
 */
public class FlowControlStrategiesTest
{
    private static final String MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost";

    private static final int STREAM_ID = 1;

    private static final int TERM_BUFFER_LENGTH = 64 * 1024;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR =
        IoUtil.tmpDirName() + "aeron-system-tests-" + UUID.randomUUID().toString() + File.separator;

    private final MediaDriver.Context driverAContext = new MediaDriver.Context();
    private final MediaDriver.Context driverBContext = new MediaDriver.Context();

    private Aeron clientA;
    private Aeron clientB;
    private MediaDriver driverA;
    private MediaDriver driverB;
    private Publication publication;
    private Subscription subscriptionA;
    private Subscription subscriptionB;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);
    private FragmentHandler fragmentHandlerA = mock(FragmentHandler.class);
    private FragmentHandler fragmentHandlerB = mock(FragmentHandler.class);

    private void launch()
    {
        final String baseDirA = ROOT_DIR + "A";
        final String baseDirB = ROOT_DIR + "B";

        buffer.putInt(0, 1);

        driverAContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .errorHandler(Throwable::printStackTrace)
            .threadingMode(ThreadingMode.SHARED);

        driverBContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .errorHandler(Throwable::printStackTrace)
            .threadingMode(ThreadingMode.SHARED);

        driverA = MediaDriver.launch(driverAContext);
        driverB = MediaDriver.launch(driverBContext);
        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverAContext.aeronDirectoryName()));
        clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverBContext.aeronDirectoryName()));
    }

    @After
    public void after()
    {
        clientB.close();
        clientA.close();
        driverB.close();
        driverA.close();

        IoUtil.delete(new File(ROOT_DIR), true);
    }

    @Test(timeout = 10_000)
    public void shouldSpinUpAndShutdown()
    {
        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Test(timeout = 10_000)
    public void shouldTimeoutImageWhenBehindForTooLongWithMaxMulticastFlowControlStrategy() throws Exception
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        final CountDownLatch unavailableCountDownLatch = new CountDownLatch(1);
        final CountDownLatch availableCountDownLatch = new CountDownLatch(2);

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driverAContext.multicastFlowControlSupplier(
            (udpChannel, streamId, registrationId) -> new MaxMulticastFlowControl());

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(
            MULTICAST_URI,
            STREAM_ID,
            (image) -> availableCountDownLatch.countDown(),
            (image) -> unavailableCountDownLatch.countDown());
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
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

            // A keeps up
            final MutableInteger fragmentsRead = new MutableInteger();
            SystemTest.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.value += subscriptionA.poll(fragmentHandlerA, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));

            fragmentsRead.set(0);

            // B receives slowly and eventually can't keep up
            if (i % 10 == 0)
            {
                SystemTest.executeUntil(
                    () -> fragmentsRead.get() > 0,
                    (j) ->
                    {
                        fragmentsRead.value += subscriptionB.poll(fragmentHandlerB, 1);
                        Thread.yield();
                    },
                    Integer.MAX_VALUE,
                    TimeUnit.MILLISECONDS.toNanos(500));
            }
        }

        unavailableCountDownLatch.await();
        availableCountDownLatch.await();

        verify(fragmentHandlerA, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));

        verify(fragmentHandlerB, atMost(numMessagesToSend - 1)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Test(timeout = 10_000)
    public void shouldSlowDownWhenBehindWithMinMulticastFlowControlStrategy()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int numMessagesLeftToSend = numMessagesToSend;
        int numFragmentsFromA = 0;
        int numFragmentsFromB = 0;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driverAContext.multicastFlowControlSupplier(
            (udpChannel, streamId, registrationId) -> new MinMulticastFlowControl());

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        for (long i = 0; numFragmentsFromA < numMessagesToSend || numFragmentsFromB < numMessagesToSend; i++)
        {
            if (numMessagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    numMessagesLeftToSend--;
                }
            }

            SystemTest.checkInterruptedStatus();
            Thread.yield();

            // A keeps up
            numFragmentsFromA += subscriptionA.poll(fragmentHandlerA, 10);

            // B receives slowly
            if ((i % 2) == 0)
            {
                numFragmentsFromB += subscriptionB.poll(fragmentHandlerB, 1);
            }
        }

        verify(fragmentHandlerA, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));

        verify(fragmentHandlerB, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Test(timeout = 10_000)
    public void shouldRemoveDeadReceiverWithMinMulticastFlowControlStrategy()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int numMessagesLeftToSend = numMessagesToSend;
        int numFragmentsFromA = 0;
        int numFragmentsFromB = 0;
        boolean isClosedB = false;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driverAContext.multicastFlowControlSupplier(
            (udpChannel, streamId, registrationId) -> new MinMulticastFlowControl());

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        while (numFragmentsFromA < numMessagesToSend)
        {
            if (numMessagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    numMessagesLeftToSend--;
                }
            }

            // A keeps up
            numFragmentsFromA += subscriptionA.poll(fragmentHandlerA, 10);

            // B receives up to 1/8 of the messages, then stops
            if (numFragmentsFromB < (numMessagesToSend / 8))
            {
                numFragmentsFromB += subscriptionB.poll(fragmentHandlerB, 10);
            }
            else if (!isClosedB)
            {
                subscriptionB.close();
                isClosedB = true;
            }
        }

        verify(fragmentHandlerA, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Test(timeout = 10_000)
    public void shouldSlowDownToSlowPreferredWithPreferredMulticastFlowControlStrategy()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int numMessagesLeftToSend = numMessagesToSend;
        int numFragmentsFromA = 0;
        int numFragmentsFromB = 0;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driverAContext.multicastFlowControlSupplier(
            (udpChannel, streamId, registrationId) -> new PreferredMulticastFlowControl());
        driverBContext.applicationSpecificFeedback(PreferredMulticastFlowControl.PREFERRED_ASF_BYTES);

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        for (long i = 0; numFragmentsFromB < numMessagesToSend || numFragmentsFromA < numMessagesToSend; i++)
        {
            if (numMessagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    numMessagesLeftToSend--;
                }
            }

            SystemTest.checkInterruptedStatus();
            Thread.yield();

            // A keeps up
            numFragmentsFromA += subscriptionA.poll(fragmentHandlerA, 10);

            // B receives slowly
            if ((i % 2) == 0)
            {
                numFragmentsFromB += subscriptionB.poll(fragmentHandlerB, 1);
            }
        }

        verify(fragmentHandlerA, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));

        verify(fragmentHandlerB, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Test(timeout = 10_000)
    public void shouldKeepUpToFastPreferredWithPreferredMulticastFlowControlStrategy()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int numMessagesLeftToSend = numMessagesToSend;
        int numFragmentsFromA = 0;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driverAContext.multicastFlowControlSupplier(
            (udpChannel, streamId, registrationId) -> new PreferredMulticastFlowControl());
        driverAContext.applicationSpecificFeedback(PreferredMulticastFlowControl.PREFERRED_ASF_BYTES);

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        for (long i = 0; numFragmentsFromA < numMessagesToSend; i++)
        {
            if (numMessagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    numMessagesLeftToSend--;
                }
            }

            SystemTest.checkInterruptedStatus();
            Thread.yield();

            // A keeps up
            numFragmentsFromA += subscriptionA.poll(fragmentHandlerA, 10);

            // B receives slowly
            if ((i % 2) == 0)
            {
                subscriptionB.poll(fragmentHandlerB, 1);
            }
        }

        verify(fragmentHandlerA, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));

        verify(fragmentHandlerB, atMost(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }

    @Test(timeout = 10_000)
    public void shouldRemoveDeadPreferredReceiverWithPreferredMulticastFlowControlStrategy()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;
        int numMessagesLeftToSend = numMessagesToSend;
        int numFragmentsReadFromA = 0, numFragmentsReadFromB = 0;
        boolean isBClosed = false;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driverAContext.multicastFlowControlSupplier(
            (udpChannel, streamId, registrationId) -> new PreferredMulticastFlowControl());
        driverBContext.applicationSpecificFeedback(PreferredMulticastFlowControl.PREFERRED_ASF_BYTES);

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        while (numFragmentsReadFromA < numMessagesToSend)
        {
            if (numMessagesLeftToSend > 0)
            {
                if (publication.offer(buffer, 0, buffer.capacity()) >= 0L)
                {
                    numMessagesLeftToSend--;
                }
            }

            // A keeps up
            numFragmentsReadFromA += subscriptionA.poll(fragmentHandlerA, 10);

            // B receives up to 1/8 of the messages, then stops
            if (numFragmentsReadFromB < (numMessagesToSend / 8))
            {
                numFragmentsReadFromB += subscriptionB.poll(fragmentHandlerB, 10);
            }
            else if (!isBClosed)
            {
                subscriptionB.close();
                isBClosed = true;
            }
        }

        verify(fragmentHandlerA, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }
}
