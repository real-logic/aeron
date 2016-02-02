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
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import uk.co.real_logic.aeron.driver.DebugReceiveChannelEndpointSupplier;
import uk.co.real_logic.aeron.driver.DebugSendChannelEndpointSupplier;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.aeron.logbuffer.FileBlockHandler;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * Test that has a publisher and subscriber and single media driver for unicast and multicast cases
 */
@RunWith(Theories.class)
public class PubAndSubTest
{
    @DataPoint
    public static final String UNICAST_URI = "aeron:udp?remote=localhost:54325";

    @DataPoint
    public static final String MULTICAST_URI = "aeron:udp?group=224.20.30.39:54326|interface=localhost";

    @DataPoint
    public static final String IPC_URI = "aeron:ipc";

    private static final int STREAM_ID = 1;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.SHARED;

    private final MediaDriver.Context context = new MediaDriver.Context();
    private final Aeron.Context publishingAeronContext = new Aeron.Context();
    private final Aeron.Context subscribingAeronContext = new Aeron.Context();

    private Aeron publishingClient;
    private Aeron subscribingClient;
    private MediaDriver driver;
    private Subscription subscription;
    private Publication publication;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[8192]);
    private FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private FileBlockHandler fileBlockHandler = mock(FileBlockHandler.class);

    private void launch(final String channel) throws Exception
    {
        context.threadingMode(THREADING_MODE);

        driver = MediaDriver.launch(context);
        publishingClient = Aeron.connect(publishingAeronContext);
        subscribingClient = Aeron.connect(subscribingAeronContext);
        publication = publishingClient.addPublication(channel, STREAM_ID);
        subscription = subscribingClient.addSubscription(channel, STREAM_ID);
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

        if (null != subscribingClient)
        {
            subscribingClient.close();
        }

        if (null != publishingClient)
        {
            publishingClient.close();
        }

        if (null != driver)
        {
            driver.close();
        }

        context.deleteAeronDirectory();
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldSpinUpAndShutdown(final String channel) throws Exception
    {
        launch(channel);
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessage(final String channel) throws Exception
    {
        launch(channel);

        publishMessage();

        final int fragmentsRead[] = new int[1];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] > 0,
            (i) ->
            {
                fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(9900));

        verify(fragmentHandler).onFragment(
            any(UnsafeBuffer.class),
            eq(HEADER_LENGTH),
            eq(SIZE_OF_INT),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessageViaPollFile(final String channel) throws Exception
    {
        launch(channel);

        publishMessage();

        final int fragmentsRead[] = new int[1];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] > 0,
            (i) ->
            {
                fragmentsRead[0] += subscription.filePoll(fileBlockHandler, Integer.MAX_VALUE);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(9900));

        final long expectedOffset = 0L;
        final int messageSize = SIZE_OF_INT;
        final int expectedLength = BitUtil.align(HEADER_LENGTH + messageSize, FRAME_ALIGNMENT);

        final ArgumentCaptor<FileChannel> channelArgumentCaptor = ArgumentCaptor.forClass(FileChannel.class);
        verify(fileBlockHandler).onBlock(
            channelArgumentCaptor.capture(),
            eq(expectedOffset),
            eq(expectedLength),
            anyInt(),
            anyInt());

        assertTrue("File Channel is closed", channelArgumentCaptor.getValue().isOpen());
    }

    private void publishMessage()
    {
        buffer.putInt(0, 1);

        while (publication.offer(buffer, 0, SIZE_OF_INT) < 0L)
        {
            Thread.yield();
        }
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldContinueAfterBufferRollover(final String channel) throws Exception
    {
        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = numMessagesInTermBuffer + 1;

        context.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldContinueAfterRolloverWithMinimalPaddingHeader(final String channel) throws Exception
    {
        final int termBufferLength = 64 * 1024;
        final int termBufferLengthMinusPaddingHeader = termBufferLength - HEADER_LENGTH;
        final int num1kMessagesInTermBuffer = 63;
        final int lastMessageLength =
            termBufferLengthMinusPaddingHeader - (num1kMessagesInTermBuffer * 1024) - HEADER_LENGTH;
        final int messageLength = 1024 - HEADER_LENGTH;

        context.publicationTermBufferLength(termBufferLength);

        launch(channel);

        // lock step reception until we get to within 8 messages of the end
        for (int i = 0; i < num1kMessagesInTermBuffer - 7; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        for (int i = 7; i > 0; i--)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }
        }

        // small enough to leave room for padding that is just a header
        while (publication.offer(buffer, 0, lastMessageLength) < 0L)
        {
            Thread.yield();
        }

        // no roll over
        while (publication.offer(buffer, 0, messageLength) < 0L)
        {
            Thread.yield();
        }

        final int fragmentsRead[] = new int[1];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] == 9,
            (j) ->
            {
                fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        final InOrder inOrder = inOrder(fragmentHandler);

        inOrder.verify(fragmentHandler, times(num1kMessagesInTermBuffer)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
        inOrder.verify(fragmentHandler, times(1)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(lastMessageLength),
            any(Header.class));
        inOrder.verify(fragmentHandler, times(1)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessageOneForOneWithDataLoss(final String channel) throws Exception
    {
        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = 2 * numMessagesInTermBuffer;

        context.publicationTermBufferLength(termBufferLength);
        context.dataLossRate(0.10);                // 10% data loss
        context.dataLossSeed(0xdeadbeefL);         // predictable seed
        context.sendChannelEndpointSupplier(new DebugSendChannelEndpointSupplier());
        context.receiveChannelEndpointSupplier(new DebugReceiveChannelEndpointSupplier());

        launch(channel);

        Assume.assumeThat(channel, not(IPC_URI));

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessageBatchedWithDataLoss(final String channel) throws Exception
    {
        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = 2 * numMessagesInTermBuffer;
        final int numBatches = 4;
        final int numMessagesPerBatch = numMessagesToSend / numBatches;

        context.publicationTermBufferLength(termBufferLength);
        context.dataLossRate(0.10);                // 10% data loss
        context.dataLossSeed(0xcafebabeL);         // predictable seed
        context.sendChannelEndpointSupplier(new DebugSendChannelEndpointSupplier());
        context.receiveChannelEndpointSupplier(new DebugReceiveChannelEndpointSupplier());

        launch(channel);

        Assume.assumeThat(channel, not(IPC_URI));

        for (int i = 0; i < numBatches; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (publication.offer(buffer, 0, messageLength) < 0L)
                {
                    Thread.yield();
                }
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] >= numMessagesPerBatch,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldContinueAfterBufferRolloverBatched(final String channel) throws Exception
    {
        final int termBufferLength = 64 * 1024;
        final int numBatchesPerTerm = 4;
        final int numMessagesPerBatch = 16;
        final int numMessagesInTermBuffer = numMessagesPerBatch * numBatchesPerTerm;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = numMessagesInTermBuffer + 1;

        context.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numBatchesPerTerm; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (publication.offer(buffer, 0, messageLength) < 0L)
                {
                    Thread.yield();
                }
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] >= numMessagesPerBatch,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        while (publication.offer(buffer, 0, messageLength) < 0L)
        {
            Thread.yield();
        }

        final int fragmentsRead[] = new int[1];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] > 0,
            (j) ->
            {
                fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(900));

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
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
        final int termBufferLength = 64 * 1024;
        final int messageLength = 1032 - HEADER_LENGTH;
        final int numMessagesToSend = 64;

        context.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
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
        final int termBufferLength = 64 * 1024;
        final int messageLength = 1032 - HEADER_LENGTH;
        final int numMessagesToSend = 64;
        final int numBatchesPerTerm = 4;
        final int numMessagesPerBatch = numMessagesToSend / numBatchesPerTerm;

        context.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numBatchesPerTerm; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (publication.offer(buffer, 0, messageLength) < 0L)
                {
                    Thread.yield();
                }
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] >= numMessagesPerBatch,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceiveOnlyAfterSendingUpToFlowControlLimit(final String channel) throws Exception
    {
        /*
         * The subscriber will flow control before an entire term buffer. So, send until can't send no 'more.
         * Then start up subscriber to drain.
         */
        final int termBufferLength = 64 * 1024;
        final int numMessagesPerTerm = 64;
        final int messageLength = (termBufferLength / numMessagesPerTerm) - HEADER_LENGTH;
        final int maxFails = 10000;
        int messagesSent = 0;

        context.publicationTermBufferLength(termBufferLength);

        launch(channel);

        for (int i = 0; i < numMessagesPerTerm; i++)
        {
            int offerFails = 0;

            while (publication.offer(buffer, 0, messageLength) < 0L)
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
                fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        verify(fragmentHandler, times(messagesToReceive)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldReceivePublishedMessageOneForOneWithReSubscription(final String channel) throws Exception
    {
        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSendStageOne = numMessagesInTermBuffer / 2;
        final int numMessagesToSendStageTwo = numMessagesInTermBuffer;
        final CountDownLatch newImageLatch = new CountDownLatch(1);
        final int stage[] = { 1 };

        context.publicationTermBufferLength(termBufferLength);
        subscribingAeronContext.availableImageHandler(
            (image) ->
            {
                if (2 == stage[0])
                {
                    newImageLatch.countDown();
                }
            });

        launch(channel);

        for (int i = 0; i < numMessagesToSendStageOne; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        subscription.close();
        stage[0] = 2;
        subscription = subscribingClient.addSubscription(channel, STREAM_ID);

        newImageLatch.await();

        for (int i = 0; i < numMessagesToSendStageTwo; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }

            final int fragmentsRead[] = new int[1];
            SystemTestHelper.executeUntil(
                () -> fragmentsRead[0] > 0,
                (j) ->
                {
                    fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                    Thread.yield();
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(fragmentHandler, times(numMessagesToSendStageOne + numMessagesToSendStageTwo)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10000)
    public void shouldFragmentExactMessageLengthsCorrectly(final String channel) throws Exception
    {
        final int termBufferLength = 64 * 1024;
        final int numFragmentsPerMessage = 2;
        final int mtuLength = 4096;
        final int frameLength = mtuLength - HEADER_LENGTH;
        final int messageLength = frameLength * numFragmentsPerMessage;
        final int numMessagesToSend = 2;
        final int numFramesToExpect = numMessagesToSend * numFragmentsPerMessage;

        context.publicationTermBufferLength(termBufferLength)
            .mtuLength(mtuLength);

        launch(channel);

        Assume.assumeThat(channel, not(IPC_URI));

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }
        }

        final int fragmentsRead[] = new int[1];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] > numFramesToExpect,
            (j) ->
            {
                fragmentsRead[0] += subscription.poll(fragmentHandler, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        verify(fragmentHandler, times(numFramesToExpect)).onFragment(
            any(UnsafeBuffer.class),
            anyInt(),
            eq(frameLength),
            any(Header.class));
    }
}
