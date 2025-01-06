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

import io.aeron.driver.MaxMulticastFlowControlSupplier;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

@ExtendWith(InterruptingTestCallback.class)
class MaxFlowControlStrategySystemTest
{
    private static final String MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";
    private static final int STREAM_ID = 1001;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR = SystemUtil.tmpDirName() + "aeron-system-tests" + File.separator;

    private final MediaDriver.Context driverAContext = new MediaDriver.Context();
    private final MediaDriver.Context driverBContext = new MediaDriver.Context();

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

    @AfterEach
    void after()
    {
        CloseHelper.quietCloseAll(clientB, clientA, driverB, driverA);
    }

    @Test
    @InterruptAfter(10)
    void shouldSpinUpAndShutdown()
    {
        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected() || !publication.isConnected())
        {
            Tests.yield();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldTimeoutImageWhenBehindForTooLongWithMaxMulticastFlowControlStrategy()
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        driverBContext.imageLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500));
        driverAContext.multicastFlowControlSupplier(new MaxMulticastFlowControlSupplier());

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() || !subscriptionB.isConnected() || !publication.isConnected())
        {
            Tests.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            fragmentsRead.set(0);

            // A keeps up
            Tests.executeUntil(
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
                Tests.executeUntil(
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

    private void launch()
    {
        final String baseDirA = ROOT_DIR + "A";
        final String baseDirB = ROOT_DIR + "B";

        buffer.putInt(0, 1);

        driverAContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .errorHandler(Tests::onError)
            .threadingMode(ThreadingMode.SHARED);

        driverBContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
            .errorHandler(Tests::onError)
            .threadingMode(ThreadingMode.SHARED);

        driverA = TestMediaDriver.launch(driverAContext, testWatcher);
        driverB = TestMediaDriver.launch(driverBContext, testWatcher);
        clientA = Aeron.connect(
            new Aeron.Context()
                .errorHandler(Tests::onError)
                .aeronDirectoryName(driverAContext.aeronDirectoryName()));

        clientB = Aeron.connect(
            new Aeron.Context()
                .errorHandler(Tests::onError)
                .aeronDirectoryName(driverBContext.aeronDirectoryName()));
    }
}
