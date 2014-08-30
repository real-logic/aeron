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
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.protocol.*;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;

/**
 * Test that has a consumer and single media driver for unicast cases. Uses socket as sender/publisher endpoint.
 */
public class SubUnicastTest
{
    private static final String HOST = "localhost";
    private static final int PORT = 54323;
    private static final int SRC_PORT = 54324;
    private static final String URI = "udp://" + HOST + ":" + PORT;
    private static final int STREAM_ID = 1;
    private static final int SESSION_ID = 2;
    private static final int TERM_ID = 3;
    private static final byte[] PAYLOAD = "Payload goes here!".getBytes();
    private static final byte[] NO_PAYLOAD = {};
    private static final int FRAME_COUNT_LIMIT = Integer.MAX_VALUE;
    private static final int ALIGNED_FRAME_LENGTH =
        BitUtil.align(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT);

    private final AtomicBuffer payload = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));

    private final InetSocketAddress rcvAddress = new InetSocketAddress(HOST, PORT);
    private final InetSocketAddress srcAddress = new InetSocketAddress(HOST, SRC_PORT);

    private Aeron consumingClient;
    private MediaDriver driver;
    private Subscription subscription;
    private DatagramChannel senderChannel;

    private final Queue<byte[]> receivedFrames = new ArrayDeque<>();
    private final DataHandler saveFrames =
        (buffer, offset, length, sessionId, flags) ->
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            receivedFrames.add(data);
        };

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    private final IntConsumer subscriptionPollWithYield =
        (i) ->
        {
            subscription.poll(FRAME_COUNT_LIMIT);
            Thread.yield();
        };

    @Before
    public void setupClientAndMediaDriver() throws Exception
    {
        payload.putBytes(0, PAYLOAD);

        senderChannel = DatagramChannel.open();
        senderChannel.configureBlocking(false);
        senderChannel.bind(srcAddress);

        final MediaDriver.Context ctx = new MediaDriver.Context();

        ctx.dirsDeleteOnExit(true);

        driver = MediaDriver.launch(ctx);
        consumingClient = Aeron.connect(newAeronContext());
        subscription = consumingClient.addSubscription(URI, STREAM_ID, saveFrames);
    }

    private Aeron.Context newAeronContext()
    {
        return new Aeron.Context();
    }

    @After
    public void closeEverything() throws Exception
    {
        if (null != subscription)
        {
            subscription.close();
        }

        consumingClient.close();
        driver.close();

        if (null != senderChannel)
        {
            senderChannel.close();
        }
    }

    @Test(timeout = 1000)
    public void shouldReceiveCorrectlyFormedSingleDataFrame() throws Exception
    {
        sendSetupFrame();

        final AtomicInteger statusMessagesSeen = new AtomicInteger();

        // should poll SM from consumer
        DatagramTestHelper.receiveUntil(
            senderChannel,
            (buffer) ->
            {
                statusMessage.wrap(buffer, 0);
                assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
                assertThat(statusMessage.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
                assertThat(statusMessage.streamId(), is(STREAM_ID));
                assertThat(statusMessage.sessionId(), is(SESSION_ID));
                assertThat(statusMessage.termId(), is(TERM_ID));
                statusMessagesSeen.incrementAndGet();

                return true;
            });

        assertThat(statusMessagesSeen.get(), greaterThanOrEqualTo(1));

        // send single Data Frame
        sendDataFrame(0, PAYLOAD);

        // ticks poll data into app
        SystemTestHelper.executeUntil(
            () -> receivedFrames.size() > 0,
            subscriptionPollWithYield,
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
    }

    @Test(timeout = 1000)
    public void shouldReceiveMultipleDataFrames() throws Exception
    {
        sendSetupFrame();

        final AtomicInteger statusMessagesSeen = new AtomicInteger();

        DatagramTestHelper.receiveUntil(
            senderChannel,
            (buffer) ->
            {
                statusMessage.wrap(buffer, 0);
                assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
                statusMessagesSeen.incrementAndGet();

                return true;
            });

        assertThat(statusMessagesSeen.get(), greaterThanOrEqualTo(1));

        for (int i = 0; i < 3; i++)
        {
            // send single Data Frame
            sendDataFrame(i * ALIGNED_FRAME_LENGTH, PAYLOAD);
        }

        // ticks poll data into app
        SystemTestHelper.executeUntil(
            () -> receivedFrames.size() >= 3,
            subscriptionPollWithYield,
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(3));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
    }

    @Test(timeout = 1000)
    public void shouldSendNaksForMissingData() throws Exception
    {
        sendSetupFrame();

        final AtomicInteger statusMessagesSeen = new AtomicInteger();
        final AtomicInteger naksSeen = new AtomicInteger();

        DatagramTestHelper.receiveUntil(
            senderChannel,
            (buffer) ->
            {
                statusMessage.wrap(buffer, 0);
                assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
                statusMessagesSeen.incrementAndGet();

                return true;
            });

        assertThat(statusMessagesSeen.get(), greaterThanOrEqualTo(1));

        sendDataFrame(0, PAYLOAD);
        sendDataFrame(2 * ALIGNED_FRAME_LENGTH, PAYLOAD);

        // ticks poll data into app
        SystemTestHelper.executeUntil(
            () -> receivedFrames.size() > 0,
            subscriptionPollWithYield,
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));

        DatagramTestHelper.receiveUntil(
            senderChannel,
            (buffer) ->
            {
                nakHeader.wrap(buffer, 0);
                assertThat(nakHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_NAK));
                assertThat(nakHeader.frameLength(), is(NakFlyweight.HEADER_LENGTH));
                assertThat(nakHeader.streamId(), is(STREAM_ID));
                assertThat(nakHeader.sessionId(), is(SESSION_ID));
                assertThat(nakHeader.termId(), is(TERM_ID));
                assertThat(nakHeader.termOffset(), is(ALIGNED_FRAME_LENGTH));
                assertThat(nakHeader.length(), is(ALIGNED_FRAME_LENGTH));
                naksSeen.incrementAndGet();

                return true;
            });

        assertThat(naksSeen.get(), greaterThanOrEqualTo(1));
    }

    @Test(timeout = 1000)
    public void shouldReceiveRetransmitAndDeliver() throws Exception
    {
        sendSetupFrame();

        final AtomicInteger statusMessagesSeen = new AtomicInteger();
        final AtomicInteger naksSeen = new AtomicInteger();

        DatagramTestHelper.receiveUntil(
            senderChannel,
            (buffer) ->
            {
                statusMessage.wrap(buffer, 0);
                assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
                statusMessagesSeen.incrementAndGet();

                return true;
            });

        assertThat(statusMessagesSeen.get(), greaterThanOrEqualTo(1));

        sendDataFrame(0, PAYLOAD);
        sendDataFrame(2 * ALIGNED_FRAME_LENGTH, PAYLOAD);

        // ticks poll data into app
        SystemTestHelper.executeUntil(
            () -> receivedFrames.size() > 0,
            subscriptionPollWithYield,
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));

        DatagramTestHelper.receiveUntil(
            senderChannel,
            (buffer) ->
            {
                nakHeader.wrap(buffer, 0);
                assertThat(nakHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_NAK));
                naksSeen.incrementAndGet();

                return true;
            });

        assertThat(naksSeen.get(), greaterThanOrEqualTo(1));

        sendDataFrame(ALIGNED_FRAME_LENGTH, PAYLOAD);

        // ticks poll data into app
        SystemTestHelper.executeUntil(
            () -> receivedFrames.size() >= 2,
            subscriptionPollWithYield,
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(500));

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(2));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
    }

    private void sendDataFrame(final int termOffset, final byte[] payload) throws Exception
    {
        final int frameLength = ALIGNED_FRAME_LENGTH;
        final ByteBuffer dataBuffer = ByteBuffer.allocate(frameLength);
        final AtomicBuffer dataAtomicBuffer = new AtomicBuffer(dataBuffer);

        dataHeader.wrap(dataAtomicBuffer, 0);
        dataHeader.termId(TERM_ID)
                  .streamId(STREAM_ID)
                  .sessionId(SESSION_ID)
                  .termOffset(termOffset)
                  .frameLength(DataHeaderFlyweight.HEADER_LENGTH + payload.length)
                  .headerType(HeaderFlyweight.HDR_TYPE_DATA)
                  .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
                  .version(HeaderFlyweight.CURRENT_VERSION);

        if (payload.length > 0)
        {
            dataAtomicBuffer.putBytes(dataHeader.dataOffset(), payload);
        }

        dataBuffer.position(0);
        dataBuffer.limit(frameLength);
        final int bytesSent = senderChannel.send(dataBuffer, rcvAddress);

        assertThat(bytesSent, is(frameLength));
    }

    private void sendSetupFrame() throws Exception
    {
        final ByteBuffer setupBuffer = ByteBuffer.allocate(SetupFlyweight.HEADER_LENGTH);
        final AtomicBuffer setupAtomicBuffer = new AtomicBuffer(setupBuffer);

        setupHeader.wrap(setupAtomicBuffer, 0);
        setupHeader.termId(TERM_ID)
                   .streamId(STREAM_ID)
                   .sessionId(SESSION_ID)
                   .frameLength(SetupFlyweight.HEADER_LENGTH)
                   .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
                   .flags((byte)0)
                   .version(HeaderFlyweight.CURRENT_VERSION);

        setupBuffer.position(0);
        setupBuffer.limit(SetupFlyweight.HEADER_LENGTH);
        final int bytesSent = senderChannel.send(setupBuffer, rcvAddress);

        assertThat(bytesSent, is(SetupFlyweight.HEADER_LENGTH));
    }
}
