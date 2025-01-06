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
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
class MultiDriverTest
{
    private static final String MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";

    private static final int STREAM_ID = 1001;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.SHARED;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR = SystemUtil.tmpDirName() + "aeron-system-tests" + File.separator;
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();
    private Aeron clientA;
    private Aeron clientB;
    private TestMediaDriver driverA;
    private TestMediaDriver driverB;
    private Publication publication;
    private Subscription subscriptionA;
    private Subscription subscriptionB;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);

    private final MutableInteger fragmentCountA = new MutableInteger();
    private final FragmentHandler fragmentHandlerA = (buffer1, offset, length, header) -> fragmentCountA.value++;
    private final MutableInteger fragmentCountB = new MutableInteger();
    private final FragmentHandler fragmentHandlerB = (buffer1, offset, length, header) -> fragmentCountB.value++;

    private void launch()
    {
        final String baseDirA = ROOT_DIR + "A";
        final String baseDirB = ROOT_DIR + "B";

        buffer.putInt(0, 1);

        final MediaDriver.Context driverAContext = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .threadingMode(THREADING_MODE);

        final MediaDriver.Context driverBContext = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .threadingMode(THREADING_MODE);

        driverA = TestMediaDriver.launch(driverAContext, testWatcher);
        testWatcher.dataCollector().add(driverA.context().aeronDirectory());
        driverB = TestMediaDriver.launch(driverBContext, testWatcher);
        testWatcher.dataCollector().add(driverB.context().aeronDirectory());
        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverAContext.aeronDirectoryName()));
        clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverBContext.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(clientA, clientB, driverA, driverB);
    }

    @Test
    @InterruptAfter(10)
    void shouldSpinUpAndShutdown()
    {
        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!subscriptionA.isConnected() && !subscriptionB.isConnected())
        {
            Tests.yield();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldJoinExistingStreamWithLockStepSendingReceiving() throws InterruptedException
    {
        final int numMessagesToSendPreJoin = NUM_MESSAGES_PER_TERM / 2;
        final int numMessagesToSendPostJoin = NUM_MESSAGES_PER_TERM;

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        for (int i = 0; i < numMessagesToSendPreJoin; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();
            Tests.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.value += subscriptionA.poll(fragmentHandlerA, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        final CountDownLatch newImageLatch = new CountDownLatch(1);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID, (image) -> newImageLatch
            .countDown(), null);

        newImageLatch.await();

        for (int i = 0; i < numMessagesToSendPostJoin; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();
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
            Tests.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.value += subscriptionB.poll(fragmentHandlerB, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        assertEquals(numMessagesToSendPreJoin + numMessagesToSendPostJoin, fragmentCountA.value);
        assertEquals(numMessagesToSendPostJoin, fragmentCountB.value);
    }

    @Test
    @InterruptAfter(10)
    void shouldJoinExistingIdleStreamWithLockStepSendingReceiving() throws InterruptedException
    {
        final int numMessagesToSendPreJoin = 0;
        final int numMessagesToSendPostJoin = NUM_MESSAGES_PER_TERM;

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!publication.isConnected() && !subscriptionA.isConnected())
        {
            Tests.yield();
        }

        final CountDownLatch newImageLatch = new CountDownLatch(1);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID, (image) -> newImageLatch
            .countDown(), null);

        newImageLatch.await();

        for (int i = 0; i < numMessagesToSendPostJoin; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Tests.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();
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
            Tests.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.value += subscriptionB.poll(fragmentHandlerB, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        assertEquals(numMessagesToSendPreJoin + numMessagesToSendPostJoin, fragmentCountA.value);
        assertEquals(numMessagesToSendPostJoin, fragmentCountB.value);
    }
}
