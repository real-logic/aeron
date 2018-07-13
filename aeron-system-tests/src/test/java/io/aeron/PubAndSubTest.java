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

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.reports.LossReport;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import io.aeron.driver.ext.DebugChannelEndpointConfiguration;
import io.aeron.driver.ext.DebugReceiveChannelEndpoint;
import io.aeron.driver.ext.DebugSendChannelEndpoint;
import io.aeron.driver.ext.LossGenerator;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * Test that has a publisher and subscriber and single media driver for unicast and multicast cases
 */
@RunWith(Theories.class)
public class PubAndSubTest
{
    @DataPoint
    public static final String UNICAST_URI = "aeron:udp?endpoint=localhost:54325";

    @DataPoint
    public static final String MULTICAST_URI = "aeron:udp?endpoint=224.20.30.39:54326|interface=localhost";

    @DataPoint
    public static final String IPC_URI = "aeron:ipc";

    private static final int STREAM_ID = 1;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.SHARED;

    private final MediaDriver.Context context = new MediaDriver.Context();

    private Aeron publishingClient;
    private Aeron subscribingClient;
    private MediaDriver driver;
    private Subscription subscription;
    private Publication publication;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[8192]);
    private FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private RawBlockHandler rawBlockHandler = mock(RawBlockHandler.class);

    private void launch(final String channel)
    {
        context
            .threadingMode(THREADING_MODE)
            .errorHandler(Throwable::printStackTrace)
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100));

        driver = MediaDriver.launch(context);
        subscribingClient = Aeron.connect();
        publishingClient = Aeron.connect();
        subscription = subscribingClient.addSubscription(channel, STREAM_ID);
        publication = publishingClient.addPublication(channel, STREAM_ID);
    }

    @After
    public void after()
    {
        CloseHelper.quietClose(publishingClient);
        CloseHelper.quietClose(subscribingClient);
        CloseHelper.quietClose(driver);

        if (null != context.aeronDirectory())
        {
            context.deleteAeronDirectory();
        }
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldReceivePublishedMessageViaPollFile(final String channel)
    {
        launch(channel);

        publishMessage();

        final MutableLong bytesRead = new MutableLong();
        SystemTest.executeUntil(
            () -> bytesRead.value > 0,
            (i) ->
            {
                final long bytes = subscription.rawPoll(rawBlockHandler, Integer.MAX_VALUE);
                if (0 == bytes)
                {
                    Thread.yield();
                }
                bytesRead.value += bytes;
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(9900));

        final long expectedOffset = 0L;
        final int expectedLength = BitUtil.align(HEADER_LENGTH + SIZE_OF_INT, FRAME_ALIGNMENT);

        final ArgumentCaptor<FileChannel> channelArgumentCaptor = ArgumentCaptor.forClass(FileChannel.class);
        verify(rawBlockHandler).onBlock(
            channelArgumentCaptor.capture(),
            eq(expectedOffset),
            any(UnsafeBuffer.class),
            eq((int)expectedOffset),
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
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldContinueAfterBufferRollover(final String channel)
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
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();

            SystemTest.executeUntil(
                () -> fragmentsRead.value > 0,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldContinueAfterRolloverWithMinimalPaddingHeader(final String channel)
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
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();

            SystemTest.executeUntil(
                () -> fragmentsRead.value > 0,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
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
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        // no roll over
        while (publication.offer(buffer, 0, messageLength) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        SystemTest.executeUntil(
            () -> fragmentsRead.value == 9,
            (j) ->
            {
                final int fragments = subscription.poll(fragmentHandler, 10);
                if (0 == fragments)
                {
                    Thread.yield();
                }
                fragmentsRead.value += fragments;
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        final InOrder inOrder = inOrder(fragmentHandler);

        inOrder.verify(fragmentHandler, times(num1kMessagesInTermBuffer)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
        inOrder.verify(fragmentHandler, times(1)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(lastMessageLength),
            any(Header.class));
        inOrder.verify(fragmentHandler, times(1)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldReceivePublishedMessageOneForOneWithDataLoss(final String channel)
    {
        if (IPC_URI.equals(channel))
        {
            return;
        }

        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = 2 * numMessagesInTermBuffer;

        final LossReport lossReport = mock(LossReport.class);
        context.lossReport(lossReport);

        final LossGenerator dataLossGenerator =
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0.10, 0xcafebabeL);
        final LossGenerator noLossGenerator =
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0, 0);

        context.publicationTermBufferLength(termBufferLength);

        context.sendChannelEndpointSupplier((udpChannel, statusIndicator, context) -> new DebugSendChannelEndpoint(
            udpChannel, statusIndicator, context, noLossGenerator, noLossGenerator));

        context.receiveChannelEndpointSupplier(
            (udpChannel, dispatcher, statusIndicator, context) -> new DebugReceiveChannelEndpoint(
            udpChannel, dispatcher, statusIndicator, context, dataLossGenerator, noLossGenerator));

        launch(channel);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                Thread.yield();
            }

            final MutableInteger mutableInteger = new MutableInteger();
            SystemTest.executeUntil(
                () -> mutableInteger.value > 0,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    mutableInteger.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));

        verify(lossReport).createEntry(anyLong(), anyLong(), anyInt(), eq(STREAM_ID), anyString(), anyString());
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldReceivePublishedMessageBatchedWithDataLoss(final String channel)
    {
        if (IPC_URI.equals(channel))
        {
            return;
        }

        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSend = 2 * numMessagesInTermBuffer;
        final int numBatches = 4;
        final int numMessagesPerBatch = numMessagesToSend / numBatches;

        final LossReport lossReport = mock(LossReport.class);
        context.lossReport(lossReport);

        final LossGenerator dataLossGenerator =
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0.10, 0xcafebabeL);
        final LossGenerator noLossGenerator =
            DebugChannelEndpointConfiguration.lossGeneratorSupplier(0, 0);

        context.publicationTermBufferLength(termBufferLength);

        context.sendChannelEndpointSupplier((udpChannel, statusIndicator, context) -> new DebugSendChannelEndpoint(
            udpChannel, statusIndicator, context, noLossGenerator, noLossGenerator));

        context.receiveChannelEndpointSupplier(
            (udpChannel, dispatcher, statusIndicator, context) -> new DebugReceiveChannelEndpoint(
            udpChannel, dispatcher, statusIndicator, context, dataLossGenerator, noLossGenerator));

        launch(channel);

        for (int i = 0; i < numBatches; i++)
        {
            for (int j = 0; j < numMessagesPerBatch; j++)
            {
                while (publication.offer(buffer, 0, messageLength) < 0L)
                {
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }
            }

            final MutableInteger fragmentsRead = new MutableInteger();

            SystemTest.executeUntil(
                () -> fragmentsRead.value >= numMessagesPerBatch,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));

        verify(lossReport).createEntry(anyLong(), anyLong(), anyInt(), eq(STREAM_ID), anyString(), anyString());
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldContinueAfterBufferRolloverBatched(final String channel)
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
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }
            }

            final MutableInteger fragmentsRead = new MutableInteger();

            SystemTest.executeUntil(
                () -> fragmentsRead.value >= numMessagesPerBatch,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        while (publication.offer(buffer, 0, messageLength) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        SystemTest.executeUntil(
            () -> fragmentsRead.value > 0,
            (j) ->
            {
                final int fragments = subscription.poll(fragmentHandler, 10);
                if (0 == fragments)
                {
                    Thread.yield();
                }
                fragmentsRead.value += fragments;
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(900));

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldContinueAfterBufferRolloverWithPadding(final String channel)
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
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();

            SystemTest.executeUntil(
                () -> fragmentsRead.value > 0,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(500));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldContinueAfterBufferRolloverWithPaddingBatched(final String channel)
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
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }
            }

            final MutableInteger fragmentsRead = new MutableInteger();

            SystemTest.executeUntil(
                () -> fragmentsRead.value >= numMessagesPerBatch,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        verify(fragmentHandler, times(numMessagesToSend)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldReceiveOnlyAfterSendingUpToFlowControlLimit(final String channel)
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

                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            if (offerFails > maxFails)
            {
                break;
            }

            messagesSent++;
        }

        final MutableInteger fragmentsRead = new MutableInteger();
        final int messagesToReceive = messagesSent;

        SystemTest.executeUntil(
            () -> fragmentsRead.value >= messagesToReceive,
            (j) ->
            {
                final int fragments = subscription.poll(fragmentHandler, 10);
                if (0 == fragments)
                {
                    Thread.yield();
                }
                fragmentsRead.value += fragments;
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        verify(fragmentHandler, times(messagesToReceive)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldReceivePublishedMessageOneForOneWithReSubscription(final String channel)
    {
        final int termBufferLength = 64 * 1024;
        final int numMessagesInTermBuffer = 64;
        final int messageLength = (termBufferLength / numMessagesInTermBuffer) - HEADER_LENGTH;
        final int numMessagesToSendStageOne = numMessagesInTermBuffer / 2;
        final int numMessagesToSendStageTwo = numMessagesInTermBuffer;

        context.publicationTermBufferLength(termBufferLength);

        launch(channel);

        while (!subscription.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        for (int i = 0; i < numMessagesToSendStageOne; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();

            SystemTest.executeUntil(
                () -> fragmentsRead.value > 0,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        assertEquals(publication.position(), subscription.imageAtIndex(0).position());

        subscription.close();
        subscription = subscribingClient.addSubscription(channel, STREAM_ID);

        while (!subscription.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        assertEquals(publication.position(), subscription.imageAtIndex(0).position());

        for (int i = 0; i < numMessagesToSendStageTwo; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }

            final MutableInteger fragmentsRead = new MutableInteger();

            SystemTest.executeUntil(
                () -> fragmentsRead.value > 0,
                (j) ->
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    if (0 == fragments)
                    {
                        Thread.yield();
                    }
                    fragmentsRead.value += fragments;
                },
                Integer.MAX_VALUE,
                TimeUnit.MILLISECONDS.toNanos(900));
        }

        assertEquals(publication.position(), subscription.imageAtIndex(0).position());

        verify(fragmentHandler, times(numMessagesToSendStageOne + numMessagesToSendStageTwo)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(messageLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldFragmentExactMessageLengthsCorrectly(final String channel)
    {
        final int termBufferLength = 64 * 1024;
        final int numFragmentsPerMessage = 2;
        final int mtuLength = context.mtuLength();
        final int frameLength = mtuLength - HEADER_LENGTH;
        final int messageLength = frameLength * numFragmentsPerMessage;
        final int numMessagesToSend = 2;
        final int numFramesToExpect = numMessagesToSend * numFragmentsPerMessage;

        context.publicationTermBufferLength(termBufferLength)
            .mtuLength(mtuLength);

        launch(channel);

        for (int i = 0; i < numMessagesToSend; i++)
        {
            while (publication.offer(buffer, 0, messageLength) < 0L)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }
        }

        final MutableInteger fragmentsRead = new MutableInteger();

        SystemTest.executeUntil(
            () -> fragmentsRead.value > numFramesToExpect,
            (j) ->
            {
                final int fragments = subscription.poll(fragmentHandler, 10);
                if (0 == fragments)
                {
                    Thread.yield();
                }
                fragmentsRead.value += fragments;
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        verify(fragmentHandler, times(numFramesToExpect)).onFragment(
            any(DirectBuffer.class),
            anyInt(),
            eq(frameLength),
            any(Header.class));
    }

    @Theory
    @Test(timeout = 10_000)
    public void shouldNoticeDroppedSubscriber(final String channel) throws Exception
    {
        launch(channel);

        while (!publication.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.sleep(1);
        }

        subscription.close();

        while (publication.isConnected())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }
}
