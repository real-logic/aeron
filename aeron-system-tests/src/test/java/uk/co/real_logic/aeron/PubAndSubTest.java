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
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.driver.MediaDriver;

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
    public static final String UNICAST_URI = "udp://localhost:54325";

    @DataPoint
    public static final String MULTICAST_URI = "udp://localhost@224.20.30.39:54326";

    private static final int STREAM_ID = 1;
    private static final int SESSION_ID = 2;

    private final MediaDriver.Context context = new MediaDriver.Context();
    private final Aeron.Context publishingAeronContext = new Aeron.Context();
    private final Aeron.Context subscribingAeronContext = new Aeron.Context();

    private Aeron publishingClient;
    private Aeron subscribingClient;
    private MediaDriver driver;
    private Subscription subscription;
    private Publication publication;

    private AtomicBuffer buffer = new AtomicBuffer(new byte[4096]);
    private DataHandler dataHandler = mock(DataHandler.class);

    private void setup(final String channel) throws Exception
    {
        context.dirsDeleteOnExit(true);

        driver = MediaDriver.launch(context);
        publishingClient = Aeron.connect(publishingAeronContext);
        subscribingClient = Aeron.connect(subscribingAeronContext);
        publication = publishingClient.addPublication(channel, STREAM_ID, SESSION_ID);
        subscription = subscribingClient.addSubscription(channel, STREAM_ID, dataHandler);
    }

    @After
    public void closeEverything() throws Exception
    {
        if (null != publication)
        {
            publication.close();
        }

        if (null != subscription)
        {
            subscription.close();
        }

        subscribingClient.close();
        publishingClient.close();
        driver.close();
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldSpinUpAndShutdown(final String channel) throws Exception
    {
        setup(channel);
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessage(final String channel) throws Exception
    {
        setup(channel);

        buffer.putInt(0, 1);

        assertTrue(publication.offer(buffer, 0, BitUtil.SIZE_OF_INT));

        final int fragmentsRead[] = new int[1];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] > 0,
            (i) ->
            {
                fragmentsRead[0] += subscription.poll(10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(900));

        verify(dataHandler).onData(
            anyObject(),
            eq(DataHeaderFlyweight.HEADER_LENGTH),
            eq(BitUtil.SIZE_OF_INT),
            eq(SESSION_ID),
            eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldContinueAfterBufferRollover(final String channel) throws Exception
    {
        final int termBufferSize = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferSize / numMessagesInTermBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
        final int numMessagesToSend = numMessagesInTermBuffer + 1;

        context.termBufferSize(termBufferSize);

        setup(channel);

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
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(dataHandler, times(numMessagesToSend)).onData(
            anyObject(),
            anyInt(),
            eq(messageLength),
            eq(SESSION_ID),
            eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessageOneForOneWithDataLoss(final String channel) throws Exception
    {
        final int termBufferSize = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferSize / numMessagesInTermBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
        final int numMessagesToSend = 2 * numMessagesInTermBuffer;

        context.termBufferSize(termBufferSize);
        context.dataLossRate(0.10);                // 10% data loss
        context.dataLossSeed(0xdeadbeefL);         // predictable seed

        setup(channel);

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
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(dataHandler, times(numMessagesToSend)).onData(
            anyObject(),
            anyInt(),
            eq(messageLength),
            eq(SESSION_ID),
            eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessageBatchedWithDataLoss(final String channel) throws Exception
    {
        final int termBufferSize = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferSize / numMessagesInTermBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
        final int numMessagesToSend = 2 * numMessagesInTermBuffer;
        final int numBatches = 4;
        final int numMessagesPerBatch = numMessagesToSend / numBatches;

        context.termBufferSize(termBufferSize);
        context.dataLossRate(0.10);                // 10% data loss
        context.dataLossSeed(0xcafebabeL);         // predictable seed

        setup(channel);

        for (int i = 0; i < numBatches; i++)
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
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(dataHandler, times(numMessagesToSend)).onData(
            anyObject(),
            anyInt(),
            eq(messageLength),
            eq(SESSION_ID),
            eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldContinueAfterBufferRolloverBatched(final String channel) throws Exception
    {
        final int termBufferSize = 64 * 1024;
        final int numBatchesPerTerm = 4;
        final int numMessagesPerBatch = 16;
        final int numMessagesInTermBuffer = numMessagesPerBatch * numBatchesPerTerm;
        final int messageLength = (termBufferSize / numMessagesInTermBuffer) - DataHeaderFlyweight.HEADER_LENGTH;
        final int numMessagesToSend = numMessagesInTermBuffer + 1;

        context.termBufferSize(termBufferSize);

        setup(channel);

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
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
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
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(900));

        verify(dataHandler, times(numMessagesToSend)).onData(
            anyObject(),
            anyInt(),
            eq(messageLength),
            eq(SESSION_ID),
            eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldContinueAfterBufferRolloverWithPadding(final String channel) throws Exception
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

        context.termBufferSize(termBufferSize);

        setup(channel);

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
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(dataHandler, times(numMessagesToSend)).onData(
            anyObject(),
            anyInt(),
            eq(messageLength),
            eq(SESSION_ID),
            eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldContinueAfterBufferRolloverWithPaddingBatched(final String channel) throws Exception
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

        context.termBufferSize(termBufferSize);

        setup(channel);

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
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(dataHandler, times(numMessagesToSend)).onData(
            anyObject(),
            anyInt(),
            eq(messageLength),
            eq(SESSION_ID),
            eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceiveOnlyAfterSendingUpToFlowControlLimit(final String channel) throws Exception
    {
        /*
         * The subscriber will flow control before an entire term buffer. So, send until can't send no 'more.
         * Then start up subscriber to drain.
         */
        final int termBufferSize = 64 * 1024;
        final int numMessagesPerTerm = 64;
        final int messageLength = (termBufferSize / numMessagesPerTerm) - DataHeaderFlyweight.HEADER_LENGTH;
        final int maxFails = 10000;
        int messagesSent = 0;

        context.termBufferSize(termBufferSize);

        setup(channel);

        for (int i = 0; i < numMessagesPerTerm; i++)
        {
            int offerFails = 0;

            while (!publication.offer(buffer, 0, messageLength))
            {
                if (++offerFails > maxFails)
                {
                    break;
                }
                Thread.yield();
            }

            if (offerFails > maxFails)
            {
                break;
            }

            messagesSent++;
        }

        final int fragmentsRead[] = new int[1];
        final int messagesToReceive = messagesSent;
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] >= messagesToReceive,
            (j) ->
            {
                fragmentsRead[0] += subscription.poll(10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        verify(dataHandler, times(messagesToReceive)).onData(
            anyObject(),
            anyInt(),
            eq(messageLength),
            eq(SESSION_ID),
            eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }
}
