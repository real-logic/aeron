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
import uk.co.real_logic.aeron.mediadriver.MediaDriver;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.NakFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static uk.co.real_logic.aeron.util.BitUtil.align;
import static uk.co.real_logic.aeron.util.CommonContext.DIRS_DELETE_ON_EXIT_PROP_NAME;

/**
 * Test that has a consumer and single media driver for unicast cases. Uses socket as sender/publisher endpoint.
 */
public class SubUnicastTest
{
    public static final EventLogger LOGGER = new EventLogger(SubUnicastTest.class);

    private static final String HOST = "localhost";
    private static final int PORT = 54323;
    private static final int SRC_PORT = 54324;
    private static final Destination DESTINATION = new Destination("udp://" + HOST + ":" + PORT);
    private static final long CHANNEL_ID = 1L;
    private static final long SESSION_ID = 2L;
    private static final long TERM_ID = 3L;
    private static final byte[] PAYLOAD = "Payload goes here!".getBytes();
    private static final byte[] NO_PAYLOAD = {};
    private static final int COUNTER_BUFFER_SZ = 1024;

    private final AtomicBuffer counterValuesBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);
    private final AtomicBuffer counterLabelsBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);

    private final AtomicBuffer payload = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));

    private final InetSocketAddress rcvAddress = new InetSocketAddress(HOST, PORT);
    private final InetSocketAddress srcAddress = new InetSocketAddress(HOST, SRC_PORT);

    private Aeron consumingClient;
    private MediaDriver driver;
    private Subscription subscription;
    private DatagramChannel senderChannel;

    private final Queue<byte[]> receivedFrames = new ArrayDeque<>();
    private final Subscription.DataHandler saveFrames =
        (buffer, offset, length, sessionId) ->
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);
            receivedFrames.add(data);
        };
    private final Subscription.NewSourceEventHandler newSource =
        (channelId, sessionId) -> System.out.println("newSource " + sessionId + " " + channelId);
    private final Subscription.InactiveSourceEventHandler inactiveSource =
        (channelId, sessionId) -> System.out.println("inactiveSource " + sessionId + " " + channelId);

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

        driver = new MediaDriver();

        consumingClient = Aeron.newSingleMediaDriver(newAeronContext());

        subscription = consumingClient.newSubscription(new Subscription.Context()
                .destination(DESTINATION)
                .channel(CHANNEL_ID, saveFrames)
                .newSourceEvent(newSource)
                .inactiveSourceEvent(inactiveSource));

        payload.putBytes(0, PAYLOAD);

        executorService = Executors.newSingleThreadExecutor();

        driver.invokeEmbedded();
        consumingClient.invoke(executorService);
    }

    private Aeron.ClientContext newAeronContext()
    {
        Aeron.ClientContext ctx = new Aeron.ClientContext();


        ctx.counterLabelsBuffer(counterLabelsBuffer)
           .counterValuesBuffer(counterValuesBuffer);

        return ctx;
    }

    @After
    public void closeEverything() throws Exception
    {
        consumingClient.shutdown();
        driver.shutdown();

        senderChannel.close();
        subscription.close();
        consumingClient.close();
        driver.close();
        executorService.shutdown();
    }

    @Test
    public void shouldReceiveCorrectlyFormedSingleDataFrame() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected and media driver set things up
        Thread.sleep(100);

        // send some 0 length data frame
        sendDataFrame(0, NO_PAYLOAD);

        // sleep so we are sure some 0 length data has been sent
        Thread.sleep(100);

        final ByteBuffer buffer = ByteBuffer.allocate(StatusMessageFlyweight.HEADER_LENGTH);
        int smsSeen = 0;

        // should receive SM from consumer
        InetSocketAddress address;
        while ((address = (InetSocketAddress)senderChannel.receive(buffer)) != null)
        {
            statusMessage.wrap(buffer, 0);
            assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
            assertThat(statusMessage.frameLength(), is(StatusMessageFlyweight.HEADER_LENGTH));
            assertThat(statusMessage.channelId(), is(CHANNEL_ID));
            assertThat(statusMessage.sessionId(), is(SESSION_ID));
            assertThat(statusMessage.termId(), is(TERM_ID));
            assertThat(buffer.position(), is(StatusMessageFlyweight.HEADER_LENGTH));
            assertThat(address, is(rcvAddress));
            buffer.clear();
            smsSeen++;
        }

        assertThat(smsSeen, greaterThanOrEqualTo(1));

        // send single Data Frame
        sendDataFrame(0, PAYLOAD);

        // sleep to make sure that the receiver thread in the media driver has a chance to receive data
        Thread.sleep(100);

        // now receive data into app
        subscription.read();

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
    }

    @Test
    public void shouldReceiveMultipleDataFrames() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected and media driver set things up
        Thread.sleep(100);

        // send some 0 length data frame
        sendDataFrame(0, NO_PAYLOAD);

        // sleep so we are sure some 0 length data has been sent
        Thread.sleep(100);

        final ByteBuffer buffer = ByteBuffer.allocate(StatusMessageFlyweight.HEADER_LENGTH);
        int smsSeen = 0;

        while (senderChannel.receive(buffer) != null)
        {
            statusMessage.wrap(buffer, 0);
            assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
            buffer.clear();
            smsSeen++;
        }

        assertThat(smsSeen, greaterThanOrEqualTo(1));

        for (int i = 0; i < 3; i++)
        {
            // send single Data Frame
            sendDataFrame(i * FrameDescriptor.FRAME_ALIGNMENT, PAYLOAD);

            // sleep to make sure that the receiver thread in the media driver has a chance to receive data
            Thread.sleep(100);
        }

        // now receive data into app
        subscription.read();

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(3));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
        assertThat(receivedFrames.remove(), is(PAYLOAD));
    }

    @Test
    public void shouldSendNaksForMissingData() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected and media driver set things up
        Thread.sleep(100);

        // send some 0 length data frame
        sendDataFrame(0, NO_PAYLOAD);

        // sleep so we are sure some 0 length data has been sent
        Thread.sleep(100);

        final ByteBuffer buffer = ByteBuffer.allocate(128);
        int smsSeen = 0, naksSeen = 0;

        while (senderChannel.receive(buffer) != null)
        {
            statusMessage.wrap(buffer, 0);
            assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
            buffer.clear();
            smsSeen++;
        }

        assertThat(smsSeen, greaterThanOrEqualTo(1));

        sendDataFrame(0, PAYLOAD);
        sendDataFrame(2 * FrameDescriptor.FRAME_ALIGNMENT, PAYLOAD);

        // sleep to make sure that the receiver thread in the media driver has a chance to receive data
        Thread.sleep(100);

        // now receive data into app
        subscription.read();

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));

        InetSocketAddress address;
        while ((address = (InetSocketAddress)senderChannel.receive(buffer)) != null)
        {
            nakHeader.wrap(buffer, 0);
            assertThat(nakHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_NAK));
            assertThat(nakHeader.frameLength(), is(NakFlyweight.HEADER_LENGTH));
            assertThat(buffer.position(), is(nakHeader.frameLength()));
            assertThat(nakHeader.channelId(), is(CHANNEL_ID));
            assertThat(nakHeader.sessionId(), is(SESSION_ID));
            assertThat(nakHeader.termId(), is(TERM_ID));
            assertThat(nakHeader.termOffset(), is((long)FrameDescriptor.FRAME_ALIGNMENT));
            assertThat(nakHeader.length(), is((long)FrameDescriptor.FRAME_ALIGNMENT));
            assertThat(address, is(rcvAddress));
            buffer.clear();
            naksSeen++;
        }

        assertThat(naksSeen, greaterThanOrEqualTo(1));
    }

    @Test
    public void shouldReceiveRetransmitAndDeliver() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected and media driver set things up
        Thread.sleep(100);

        // send some 0 length data frame
        sendDataFrame(0, NO_PAYLOAD);

        // sleep so we are sure some 0 length data has been sent
        Thread.sleep(100);

        final ByteBuffer buffer = ByteBuffer.allocate(128);
        int smsSeen = 0, naksSeen = 0;

        // should receive SM from consumer
        while (senderChannel.receive(buffer) != null)
        {
            statusMessage.wrap(buffer, 0);
            assertThat(statusMessage.headerType(), is(HeaderFlyweight.HDR_TYPE_SM));
            buffer.clear();
            smsSeen++;
        }

        assertThat(smsSeen, greaterThanOrEqualTo(1));

        sendDataFrame(0, PAYLOAD);
        sendDataFrame(2 * FrameDescriptor.FRAME_ALIGNMENT, PAYLOAD);

        // sleep to make sure that the receiver thread in the media driver has a chance to receive data
        Thread.sleep(100);

        // now receive data into app
        subscription.read();

        // assert the received Data Frames are correct
        assertThat(receivedFrames.size(), is(1));
        assertThat(receivedFrames.remove(), is(PAYLOAD));

        InetSocketAddress address;
        while ((address = (InetSocketAddress)senderChannel.receive(buffer)) != null)
        {
            nakHeader.wrap(buffer, 0);
            assertThat(nakHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_NAK));
            assertThat(address, is(rcvAddress));
            buffer.clear();
            naksSeen++;
        }

        assertThat(naksSeen, greaterThanOrEqualTo(1));

        sendDataFrame(FrameDescriptor.FRAME_ALIGNMENT, PAYLOAD);

        Thread.sleep(100);

        subscription.read();

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
