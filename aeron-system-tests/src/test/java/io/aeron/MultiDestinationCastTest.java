/*
 * Copyright 2017 Real Logic Ltd.
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
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class MultiDestinationCastTest
{
    private static final String PUB_MDC_DYNAMIC_URI = "aeron:udp?control=localhost:54325";
    private static final String SUB1_MDC_DYNAMIC_URI = "aeron:udp?endpoint=localhost:54326|control=localhost:54325";
    private static final String SUB2_MDC_DYNAMIC_URI = "aeron:udp?endpoint=localhost:54327|control=localhost:54325";

    private static final String PUB_MDC_MANUAL_URI = "aeron:udp?control=localhost:54325|control-mode=manual";
    private static final String SUB1_MDC_MANUAL_URI = "aeron:udp?endpoint=localhost:54326";
    private static final String SUB2_MDC_MANUAL_URI = "aeron:udp?endpoint=localhost:54327";

    private static final int STREAM_ID = 1;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.SHARED;

    private static final int TERM_BUFFER_LENGTH = 64 * 1024;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR =
        IoUtil.tmpDirName() + "aeron-system-tests-" + UUID.randomUUID().toString() + File.separator;

    private final MediaDriver.Context driverAContext = new MediaDriver.Context();
    private final MediaDriver.Context driverBContext = new MediaDriver.Context();
    private final Aeron.Context aeronAContext = new Aeron.Context();
    private final Aeron.Context aeronBContext = new Aeron.Context();

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
            .threadingMode(THREADING_MODE);

        aeronAContext.aeronDirectoryName(driverAContext.aeronDirectoryName());

        driverBContext.publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .threadingMode(THREADING_MODE);

        aeronBContext.aeronDirectoryName(driverBContext.aeronDirectoryName());

        driverA = MediaDriver.launch(driverAContext);
        driverB = MediaDriver.launch(driverBContext);
        clientA = Aeron.connect(aeronAContext);
        clientB = Aeron.connect(aeronBContext);
    }

    @After
    public void closeEverything()
    {
        CloseHelper.close(publication);
        CloseHelper.close(subscriptionA);
        CloseHelper.close(subscriptionB);

        CloseHelper.close(clientB);
        CloseHelper.close(clientA);
        CloseHelper.close(driverB);
        CloseHelper.close(driverA);

        IoUtil.delete(new File(ROOT_DIR), true);
    }

    @Test(timeout = 10000)
    public void shouldSpinUpAndShutdownWithDynamic() throws Exception
    {
        launch();

        publication = clientA.addPublication(PUB_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionA = clientA.addSubscription(SUB1_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(SUB2_MDC_DYNAMIC_URI, STREAM_ID);

        while (subscriptionA.hasNoImages() && subscriptionB.hasNoImages())
        {
            Thread.sleep(1);
        }
    }

    @Test(timeout = 10000)
    public void shouldSpinUpAndShutdownWithManual() throws Exception
    {
        launch();

        publication = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        subscriptionA = clientA.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
        subscriptionB = clientA.addSubscription(SUB2_MDC_MANUAL_URI, STREAM_ID);

        publication.addDestination(SUB1_MDC_MANUAL_URI);
        publication.addDestination(SUB2_MDC_MANUAL_URI);

        while (subscriptionA.hasNoImages() && subscriptionB.hasNoImages())
        {
            Thread.sleep(1);
        }
    }

    @Test(timeout = 10000)
    public void shouldSendToTwoPortsWithDynamic() throws Exception
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch();

        publication = clientA.addPublication(PUB_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionA = clientA.addSubscription(SUB1_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(SUB2_MDC_DYNAMIC_URI, STREAM_ID);

        while (subscriptionA.hasNoImages() && subscriptionB.hasNoImages())
        {
            Thread.sleep(1);
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
                    fragmentsRead.getAndAdd(subscriptionA.poll(fragmentHandlerA, 10));
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));

            fragmentsRead.set(0);
            SystemTestHelper.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.addAndGet(subscriptionB.poll(fragmentHandlerB, 10));
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
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

    @Test(timeout = 10000)
    public void shouldSendToTwoPortsWithDynamicSingleDriver() throws Exception
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch();

        publication = clientA.addPublication(PUB_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionA = clientA.addSubscription(SUB1_MDC_DYNAMIC_URI, STREAM_ID);
        subscriptionB = clientA.addSubscription(SUB2_MDC_DYNAMIC_URI, STREAM_ID);

        while (subscriptionA.hasNoImages() && subscriptionB.hasNoImages())
        {
            Thread.sleep(1);
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
                    fragmentsRead.getAndAdd(subscriptionA.poll(fragmentHandlerA, 10));
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));

            fragmentsRead.set(0);
            SystemTestHelper.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.addAndGet(subscriptionB.poll(fragmentHandlerB, 10));
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
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

    @Test(timeout = 10000)
    public void shouldSendToTwoPortsWithManualSingleDriver() throws Exception
    {
        final int numMessagesToSend = NUM_MESSAGES_PER_TERM * 3;

        launch();

        publication = clientA.addPublication(PUB_MDC_MANUAL_URI, STREAM_ID);
        subscriptionA = clientA.addSubscription(SUB1_MDC_MANUAL_URI, STREAM_ID);
        subscriptionB = clientA.addSubscription(SUB2_MDC_MANUAL_URI, STREAM_ID);

        publication.addDestination(SUB1_MDC_MANUAL_URI);
        publication.addDestination(SUB2_MDC_MANUAL_URI);

        while (subscriptionA.hasNoImages() && subscriptionB.hasNoImages())
        {
            Thread.sleep(1);
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
                    fragmentsRead.getAndAdd(subscriptionA.poll(fragmentHandlerA, 10));
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));

            fragmentsRead.set(0);
            SystemTestHelper.executeUntil(
                () -> fragmentsRead.get() > 0,
                (j) ->
                {
                    fragmentsRead.addAndGet(subscriptionB.poll(fragmentHandlerB, 10));
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
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
}
