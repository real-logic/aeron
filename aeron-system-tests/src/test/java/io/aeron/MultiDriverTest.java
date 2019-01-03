/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.collections.MutableInteger;
import org.junit.After;
import org.junit.Test;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiDriverTest
{
    private static final String MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost";

    private static final int STREAM_ID = 1;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.SHARED;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR =
        IoUtil.tmpDirName() + "aeron-system-tests-" + UUID.randomUUID().toString() + File.separator;

    private Aeron clientA;
    private Aeron clientB;
    private MediaDriver driverA;
    private MediaDriver driverB;
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
            .errorHandler(Throwable::printStackTrace)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .threadingMode(THREADING_MODE);

        final MediaDriver.Context driverBContext = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace)
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .threadingMode(THREADING_MODE);

        driverA = MediaDriver.launch(driverAContext);
        driverB = MediaDriver.launch(driverBContext);
        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverAContext.aeronDirectoryName()));
        clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverBContext.aeronDirectoryName()));
    }

    @After
    public void after()
    {
        publication.close();
        subscriptionA.close();
        subscriptionB.close();

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

        while (!subscriptionA.isConnected() && !subscriptionB.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Test(timeout = 10_000)
    public void shouldJoinExistingStreamWithLockStepSendingReceiving() throws Exception
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
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

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
        }

        final CountDownLatch newImageLatch = new CountDownLatch(1);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID, (image) -> newImageLatch.countDown(), null);

        newImageLatch.await();

        for (int i = 0; i < numMessagesToSendPostJoin; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

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
            SystemTest.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.value += subscriptionB.poll(fragmentHandlerB, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        assertThat(fragmentCountA.value, is(numMessagesToSendPreJoin + numMessagesToSendPostJoin));
        assertThat(fragmentCountB.value, is(numMessagesToSendPostJoin));
    }

    @Test(timeout = 10_000)
    public void shouldJoinExistingIdleStreamWithLockStepSendingReceiving() throws Exception
    {
        final int numMessagesToSendPreJoin = 0;
        final int numMessagesToSendPostJoin = NUM_MESSAGES_PER_TERM;

        launch();

        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID);

        while (!publication.isConnected() && !subscriptionA.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        final CountDownLatch newImageLatch = new CountDownLatch(1);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID, (image) -> newImageLatch.countDown(), null);

        newImageLatch.await();

        for (int i = 0; i < numMessagesToSendPostJoin; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

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
            SystemTest.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.value += subscriptionB.poll(fragmentHandlerB, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        assertThat(fragmentCountA.value, is(numMessagesToSendPreJoin + numMessagesToSendPostJoin));
        assertThat(fragmentCountB.value, is(numMessagesToSendPostJoin));
    }
}
