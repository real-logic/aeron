/*
 * Copyright 2014 Real Logic Ltd.
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
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test that has a publisher and subscriber and single media driver for unicast and multicast cases
 */
@RunWith(Theories.class)
public class PubAndSubTest
{
    @DataPoint
    public static final String UNICAST_DESTINATION = "udp://localhost:54325";

    @DataPoint
    public static final String MULTICAST_DESTINATION = "udp://localhost@224.20.30.39:54326";

    private static final int CHANNEL_ID = 1;
    private static final int SESSION_ID = 2;

    private final MediaDriver.DriverContext driverContext = new MediaDriver.DriverContext();
    private final Aeron.ClientContext publishingAeronContext = new Aeron.ClientContext();
    private final Aeron.ClientContext subscribingAeronContext = new Aeron.ClientContext();

    private Aeron publishingClient;
    private Aeron subscribingClient;
    private MediaDriver driver;
    private Subscription subscription;
    private Publication publication;

    private AtomicBuffer buffer = new AtomicBuffer(new byte[4096]);
    private DataHandler dataHandler = mock(DataHandler.class);

    private ExecutorService executorService;

    private void setup(final String destination) throws Exception
    {
        executorService = Executors.newFixedThreadPool(2);

        driverContext.dirsDeleteOnExit(true);
        driverContext.warnIfDirectoriesExist(false);

        driver = new MediaDriver(driverContext);

        publishingClient = Aeron.newClient(publishingAeronContext);
        subscribingClient = Aeron.newClient(subscribingAeronContext);

        driver.invokeEmbedded();
        publishingClient.invoke(executorService);
        subscribingClient.invoke(executorService);

        publication = publishingClient.addPublication(destination, CHANNEL_ID, SESSION_ID);
        subscription = subscribingClient.addSubscription(destination, CHANNEL_ID, dataHandler);
    }

    @After
    public void closeEverything() throws Exception
    {
        publication.release();
        subscription.close();

        subscribingClient.shutdown();
        publishingClient.shutdown();
        driver.shutdown();

        subscribingClient.close();
        publishingClient.close();
        driver.close();
        executorService.shutdown();
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldSpinUpAndShutdown(final String destination) throws Exception
    {
        EventLogger.logInvocation();

        setup(destination);
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldReceivePublishedMessage(final String destination) throws Exception
    {
        EventLogger.logInvocation();

        setup(destination);

        buffer.putInt(0, 1);

        assertTrue(publication.offer(buffer, 0, BitUtil.SIZE_OF_INT));

        final int fragmentsRead[] = new int[1];
        SystemTestHelper.executeUntil
            (() -> fragmentsRead[0] > 0,
             (i) ->
             {
                 fragmentsRead[0] += subscription.poll(10);
                 Thread.yield();
             },
             Integer.MAX_VALUE, TimeUnit.MILLISECONDS.toNanos(500));

        verify(dataHandler)
            .onData(anyObject(),
                eq(DataHeaderFlyweight.HEADER_LENGTH),
                eq(BitUtil.SIZE_OF_INT),
                eq(SESSION_ID),
                eq((byte) DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldContinueAfterBufferRollover(final String destination) throws Exception
    {
        final int termBufferSize = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferSize / numMessagesInTermBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
        final int numMessagesToSend = numMessagesInTermBuffer + 1;

        driverContext.termBufferSize(termBufferSize);

        setup(destination);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (!publication.offer(buffer, 0, messageLength))
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(10);
                    Thread.yield();
                },
                Integer.MAX_VALUE, TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(dataHandler, times(numMessagesToSend))
            .onData(anyObject(),
                anyInt(),
                eq(messageLength),
                eq(SESSION_ID),
                eq((byte) DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldContinueAfterBufferRolloverBatched(final String destination) throws Exception
    {
        final int termBufferSize = 64 * 1024;
        final int numBatchesPerTerm = 4;
        final int numMessagesPerBatch = 16;
        final int numMessagesInTermBuffer = numMessagesPerBatch * numBatchesPerTerm;
        final int messageLength = (termBufferSize / numMessagesInTermBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
        final int numMessagesToSend = numMessagesInTermBuffer + 1;

        driverContext.termBufferSize(termBufferSize);

        setup(destination);

        for (int i = 0; i < numBatchesPerTerm; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (!publication.offer(buffer, 0, messageLength))
                {
                    Thread.yield();
                }
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] >= numMessagesPerBatch,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(10);
                    Thread.yield();
                },
                Integer.MAX_VALUE, TimeUnit.MILLISECONDS.toNanos(900));
        }

        while (!publication.offer(buffer, 0, messageLength))
        {
            Thread.yield();
        }

        final int fragmentsRead[] = new int[1];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] > 0,
            (j) ->
            {
                fragmentsRead[0] += subscription.poll(10);
                Thread.yield();
            },
            Integer.MAX_VALUE, TimeUnit.MILLISECONDS.toNanos(900));

        verify(dataHandler, times(numMessagesToSend))
            .onData(anyObject(),
                    anyInt(),
                    eq(messageLength),
                    eq(SESSION_ID),
                    eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldContinueAfterBufferRolloverWithPadding(final String destination) throws Exception
    {
        /*
         * 65536 bytes in the buffer
         * 63 * 1032 = 65016
         * 65536 - 65016 = 520 bytes padding at the end
         * so, sending 64 messages causes last to overflow
         */
        final int termBufferSize = 64 * 1024;
        final int messageLength = 1032 - DataHeaderFlyweight.HEADER_LENGTH;
        final int numMessagesToSend = 64;

        driverContext.termBufferSize(termBufferSize);

        setup(destination);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (!publication.offer(buffer, 0, messageLength))
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(10);
                    Thread.yield();
                },
                Integer.MAX_VALUE, TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(dataHandler, times(numMessagesToSend))
            .onData(anyObject(),
                anyInt(),
                eq(messageLength),
                eq(SESSION_ID),
                eq((byte) DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldContinueAfterBufferRolloverWithPaddingBatched(final String destination) throws Exception
    {
        /*
         * 65536 bytes in the buffer
         * 63 * 1032 = 65016
         * 65536 - 65016 = 520 bytes padding at the end
         * so, sending 64 messages causes last to overflow
         */
        final int termBufferSize = 64 * 1024;
        final int messageLength = 1032 - DataHeaderFlyweight.HEADER_LENGTH;
        final int numMessagesToSend = 64;
        final int numBatchesPerTerm = 4;
        final int numMessagesPerBatch = numMessagesToSend / numBatchesPerTerm;

        driverContext.termBufferSize(termBufferSize);

        setup(destination);

        for (int i = 0; i < numBatchesPerTerm; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (!publication.offer(buffer, 0, messageLength))
                {
                    Thread.yield();
                }
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] >= numMessagesPerBatch,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(10);
                    Thread.yield();
                },
                Integer.MAX_VALUE, TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(dataHandler, times(numMessagesToSend))
            .onData(anyObject(),
                anyInt(),
                eq(messageLength),
                eq(SESSION_ID),
                eq((byte) DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }
}
