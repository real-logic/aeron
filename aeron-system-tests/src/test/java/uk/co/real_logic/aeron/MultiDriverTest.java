/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests requiring multiple embedded drivers
 */
public class MultiDriverTest
{
    public static final String MULTICAST_URI = "udp://localhost@224.20.30.39:54326";

    private static final int STREAM_ID = 1;
    private static final int SESSION_ID = 2;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.SHARED;

    private static final int TERM_BUFFER_SIZE = 64 * 1024;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_SIZE / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
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

        driverAContext.termBufferLength(TERM_BUFFER_SIZE);
        driverAContext.dirsDeleteOnExit(true);
        driverAContext.dirName(baseDirA);
        driverAContext.threadingMode(THREADING_MODE);

        aeronAContext.dirName(driverAContext.dirName());

        driverBContext.termBufferLength(TERM_BUFFER_SIZE);
        driverBContext.dirsDeleteOnExit(true);
        driverBContext.dirName(baseDirB);
        driverBContext.threadingMode(THREADING_MODE);

        aeronBContext.dirName(driverBContext.dirName());

        driverA = MediaDriver.launch(driverAContext);
        driverB = MediaDriver.launch(driverBContext);
        clientA = Aeron.connect(aeronAContext);
        clientB = Aeron.connect(aeronBContext);
    }

    @After
    public void closeEverything()
    {
        if (null != publication)
        {
            publication.close();
        }

        if (null != subscriptionA)
        {
            subscriptionA.close();
        }

        if (null != subscriptionB)
        {
            subscriptionB.close();
        }

        clientB.close();
        clientA.close();
        driverB.close();
        driverA.close();

        IoUtil.delete(new File(ROOT_DIR), true);
    }

    @Test(timeout = 10000)
    public void shouldSpinUpAndShutdown() throws Exception
    {
        launch();

        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID, SESSION_ID);
        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);
        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);

        Thread.sleep(20); // allow for connections to be established
    }

    @Test(timeout = 10000)
    public void shouldJoinExistingStreamWithLockStepSendingReceiving() throws Exception
    {
        final int numMessagesToSendPreJoin = NUM_MESSAGES_PER_TERM / 2;
        final int numMessagesToSendPostJoin = NUM_MESSAGES_PER_TERM;
        final CountDownLatch newConnectionLatch = new CountDownLatch(1);

        aeronBContext.newConnectionHandler((channel, streamId, sessionId, position, info) -> newConnectionLatch.countDown());

        launch();

        publication = clientA.addPublication(MULTICAST_URI, STREAM_ID, SESSION_ID);
        subscriptionA = clientA.addSubscription(MULTICAST_URI, STREAM_ID);

        for (int i = 0; i < numMessagesToSendPreJoin; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscriptionA.poll(fragmentHandlerA, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        subscriptionB = clientB.addSubscription(MULTICAST_URI, STREAM_ID);

        // wait until new subscriber gets new connection indication
        newConnectionLatch.await();

        for (int i = 0; i < numMessagesToSendPostJoin; i++)
        {
            while (publication.offer(buffer, 0, buffer.capacity()) < 0L)
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscriptionA.poll(fragmentHandlerA, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));

            fragmentsRead[0] = 0;
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscriptionB.poll(fragmentHandlerB, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(fragmentHandlerA, times(numMessagesToSendPreJoin + numMessagesToSendPostJoin)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));

        verify(fragmentHandlerB, times(numMessagesToSendPostJoin)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(MESSAGE_LENGTH),
            any(Header.class));
    }
}
