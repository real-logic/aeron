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

import org.junit.*;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.*;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static uk.co.real_logic.aeron.common.BitUtil.align;
import static uk.co.real_logic.aeron.common.CommonContext.DIRS_DELETE_ON_EXIT_PROP_NAME;

/**
 * Test that has a consumer and single media driver for unicast cases. Uses socket as sender/publisher endpoint.
 */
public class SubUnicastTest
{
    public static final EventLogger LOGGER = new EventLogger(SubUnicastTest.class);

    private static final String HOST = "localhost";
    private static final int PORT = 54323;
    private static final int SRC_PORT = 54324;
    private static final String DESTINATION = "udp://" + HOST + ":" + PORT;
    private static final long CHANNEL_ID = 1L;
    private static final long SESSION_ID = 2L;
    private static final long TERM_ID = 3L;
    private static final byte[] PAYLOAD = "Payload goes here!".getBytes();
    private static final byte[] NO_PAYLOAD = {};
    private static final int FRAME_COUNT_LIMIT = Integer.MAX_VALUE;

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

    private ExecutorService executorService;

    @Before
    public void setupClientAndMediaDriver() throws Exception
    {
        System.setProperty(DIRS_DELETE_ON_EXIT_PROP_NAME, "true");

        senderChannel = DatagramChannel.open();
        senderChannel.configureBlocking(false);
        senderChannel.bind(srcAddress);

        final MediaDriver.DriverContext ctx = new MediaDriver.DriverContext();

        ctx.warnIfDirectoriesExist(false);

        driver = new MediaDriver(ctx);

        consumingClient = Aeron.newSingleMediaDriver(newAeronContext());

        payload.putBytes(0, PAYLOAD);

        executorService = Executors.newSingleThreadExecutor();

        driver.invokeEmbedded();
        consumingClient.invoke(executorService);

        subscription = consumingClient.addSubscription(DESTINATION, CHANNEL_ID, saveFrames);
    }

    private Aeron.ClientContext newAeronContext()
    {
        Aeron.ClientContext ctx = new Aeron.ClientContext();

        return ctx;
    }

    @After
    public void closeEverything() throws Exception
    {
        consumingClient.shutdown();
        driver.shutdown();

        senderChannel.close();
        subscription.release();
        consumingClient.close();
        driver.close();
        executorService.shutdown();
    }

    @Test(timeout = 1000)
    public void shouldReceiveCorrectlyFormedSingleDataFrame() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected and media driver set things up
        Thread.sleep(10);

        // send some 0 length data frame
        sendDataFrame(0, NO_PAYLOAD);

        final AtomicLong statusMessagesSeen = new AtomicLong();

        // should poll SM from consumer
        DatagramTestHelper.receiveUntil(senderChannel,
            (buffer) ->
            {
                statusMessage.wrap(buffer, 0);
                assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
                assertThat(statusMessage.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
                assertThat(statusMessage.channelId(), is(CHANNEL_ID));
                assertThat(statusMessage.sessionId(), is(SESSION_ID));
                assertThat(statusMessage.termId(), is(TERM_ID));
                statusMessagesSeen.incrementAndGet();
                return true;
            });

        assertThat(statusMessagesSeen.get(), greaterThanOrEqualTo(1L));

        // send single Data Frame
        sendDataFrame(0, PAYLOAD);

        // now poll data into app
        while (0 == subscription.poll(FRAME_COUNT_LIMIT))
        {
            Thread.yield();
        }

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
    }

    @Test(timeout = 1000)
    public void shouldReceiveMultipleDataFrames() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected and media driver set things up
        Thread.sleep(10);

        // send some 0 length data frame
        sendDataFrame(0, NO_PAYLOAD);

        final AtomicLong statusMessagesSeen = new AtomicLong();

        DatagramTestHelper.receiveUntil(senderChannel,
            (buffer) ->
            {
                statusMessage.wrap(buffer, 0);
                assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
                statusMessagesSeen.incrementAndGet();
                return true;
            });

        assertThat(statusMessagesSeen.get(), greaterThanOrEqualTo(1L));

        for (int i = 0; i < 3; i++)
        {
            // send single Data Frame
            sendDataFrame(i * FrameDescriptor.FRAME_ALIGNMENT, PAYLOAD);
        }

        int rcvedMessages = 0;
        do
        {
            rcvedMessages += subscription.poll(FRAME_COUNT_LIMIT);
        }
        while (rcvedMessages < 3);

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(3));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
    }

    @Test(timeout = 1000)
    public void shouldSendNaksForMissingData() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected and media driver set things up
        Thread.sleep(10);

        // send some 0 length data frame
        sendDataFrame(0, NO_PAYLOAD);

        final AtomicLong statusMessagesSeen = new AtomicLong();
        final AtomicLong naksSeen = new AtomicLong();

        DatagramTestHelper.receiveUntil(senderChannel,
            (buffer) ->
            {
                statusMessage.wrap(buffer, 0);
                assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
                statusMessagesSeen.incrementAndGet();
                return true;
            });

        assertThat(statusMessagesSeen.get(), greaterThanOrEqualTo(1L));

        sendDataFrame(0, PAYLOAD);
        sendDataFrame(2 * FrameDescriptor.FRAME_ALIGNMENT, PAYLOAD);

        // now poll data into app
        while (0 == subscription.poll(FRAME_COUNT_LIMIT))
        {
            Thread.yield();
        }

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));

        DatagramTestHelper.receiveUntil(senderChannel,
            (buffer) ->
            {
                nakHeader.wrap(buffer, 0);
                assertThat(nakHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_NAK));
                assertThat(nakHeader.frameLength(), is(NakFlyweight.HEADER_LENGTH));
                assertThat(nakHeader.channelId(), is(CHANNEL_ID));
                assertThat(nakHeader.sessionId(), is(SESSION_ID));
                assertThat(nakHeader.termId(), is(TERM_ID));
                assertThat(nakHeader.termOffset(), is((long)FrameDescriptor.FRAME_ALIGNMENT));
                assertThat(nakHeader.length(), is((long)FrameDescriptor.FRAME_ALIGNMENT));
                naksSeen.incrementAndGet();
                return true;
            });

        assertThat(naksSeen.get(), greaterThanOrEqualTo(1L));
    }

    @Test(timeout = 1000)
    public void shouldReceiveRetransmitAndDeliver() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected and media driver set things up
        Thread.sleep(10);

        // send some 0 length data frame
        sendDataFrame(0, NO_PAYLOAD);

        final AtomicLong statusMessagesSeen = new AtomicLong();
        final AtomicLong naksSeen = new AtomicLong();

        DatagramTestHelper.receiveUntil(senderChannel,
            (buffer) ->
            {
                statusMessage.wrap(buffer, 0);
                assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
                statusMessagesSeen.incrementAndGet();
                return true;
            });

        assertThat(statusMessagesSeen.get(), greaterThanOrEqualTo(1L));

        sendDataFrame(0, PAYLOAD);
        sendDataFrame(2 * FrameDescriptor.FRAME_ALIGNMENT, PAYLOAD);

        // now poll data into app
        while (0 == subscription.poll(FRAME_COUNT_LIMIT))
        {
            Thread.yield();
        }

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));

        DatagramTestHelper.receiveUntil(senderChannel,
            (buffer) ->
            {
                nakHeader.wrap(buffer, 0);
                assertThat(nakHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_NAK));
                naksSeen.incrementAndGet();
                return true;
            });

        assertThat(naksSeen.get(), greaterThanOrEqualTo(1L));

        sendDataFrame(FrameDescriptor.FRAME_ALIGNMENT, PAYLOAD);

        while (0 == subscription.poll(FRAME_COUNT_LIMIT))
        {
            Thread.yield();
        }

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(2));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
    }

    private void sendDataFrame(final long termOffset, final byte[] payload) throws Exception
    {
        final int frameLength = align(DataHeaderFlyweight.HEADER_LENGTH + payload.length,
                                      FrameDescriptor.FRAME_ALIGNMENT);
        final ByteBuffer dataBuffer = ByteBuffer.allocate(frameLength);
        final AtomicBuffer dataAtomicBuffer = new AtomicBuffer(dataBuffer);

        dataHeader.wrap(dataAtomicBuffer, 0);
        dataHeader.termId(TERM_ID)
                  .channelId(CHANNEL_ID)
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
}
