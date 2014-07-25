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
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.common.protocol.NakFlyweight;
import uk.co.real_logic.aeron.common.protocol.StatusMessageFlyweight;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static uk.co.real_logic.aeron.common.BitUtil.align;

/**
 * Test that has a publisher and single media driver for unicast cases. Uses socket as receiver/consumer endpoint.
 */
public class PubUnicastTest
{
    private static final String HOST = "localhost";
    private static final int PORT = 54321;
    private static final int SRC_PORT = 54322;
    private static final String DESTINATION = "udp://" + HOST + ":" + SRC_PORT + "@" + HOST + ":" + PORT;
    private static final int CHANNEL_ID = 1;
    private static final int SESSION_ID = 2;
    private static final byte[] PAYLOAD = "Payload goes here!".getBytes();
    private static final int TERM_BUFFER_SIZE = LogBufferDescriptor.MIN_LOG_SIZE;
    public static final int ALIGNED_FRAME_LENGTH =
        align(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT);

    private final AtomicBuffer payload = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));

    private final InetSocketAddress srcAddress = new InetSocketAddress(HOST, SRC_PORT);

    private Aeron producingClient;
    private MediaDriver driver;
    private Publication publication;

    private DatagramChannel receiverChannel;

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();

    private ExecutorService executorService;

    @Before
    public void setupClientAndMediaDriver() throws Exception
    {
        executorService = Executors.newSingleThreadExecutor();

        receiverChannel = DatagramChannel.open();
        receiverChannel.configureBlocking(false);
        receiverChannel.bind(new InetSocketAddress(HOST, PORT));

        final MediaDriver.DriverContext ctx = new MediaDriver.DriverContext();

        ctx.termBufferSize(TERM_BUFFER_SIZE);
        ctx.dirsDeleteOnExit(true);
        ctx.warnIfDirectoriesExist(false);

        driver = new MediaDriver(ctx);

        producingClient = Aeron.newClient(newAeronContext());

        payload.putBytes(0, PAYLOAD);

        driver.invokeEmbedded();
        producingClient.invoke(executorService);

        publication = producingClient.addPublication(DESTINATION, CHANNEL_ID, SESSION_ID);
    }

    private Aeron.ClientContext newAeronContext()
    {
        return new Aeron.ClientContext();
    }

    @After
    public void closeEverything() throws Exception
    {
        publication.release();

        producingClient.shutdown();
        driver.shutdown();

        receiverChannel.close();
        producingClient.close();
        driver.close();
        executorService.shutdown();
    }

    @Test(timeout = 1000)
    public void shouldSendCorrectlyFormedSingleDataFrames() throws Exception
    {
        EventLogger.logInvocation();

        // let buffers get connected

        // this will not be sent yet
        while (!publication.offer(payload, 0, PAYLOAD.length))
        {
            Thread.yield();
        }

        final AtomicInteger termId = new AtomicInteger();
        final AtomicInteger receivedZeroLengthData = new AtomicInteger();
        final AtomicInteger receivedDataFrames = new AtomicInteger();

        // should only see 0 length data until SM is sent
        DatagramTestHelper.receiveUntil(receiverChannel,
            (buffer) ->
            {
                dataHeader.wrap(buffer, 0);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
                assertThat(dataHeader.channelId(), is(CHANNEL_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(buffer.position(), is(DataHeaderFlyweight.HEADER_LENGTH));
                termId.set(dataHeader.termId());
                receivedZeroLengthData.incrementAndGet();
                return true;
            });

        assertThat(receivedZeroLengthData.get(), greaterThanOrEqualTo(1));

        // send SM
        sendSM(termId.get());

        // assert the received Data Frames are correctly formed
        DatagramTestHelper.receiveUntil(receiverChannel,
            (buffer) ->
            {
                dataHeader.wrap(buffer, 0);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.channelId(), is(CHANNEL_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(dataHeader.termId(), is(termId.get()));

                if (dataHeader.frameLength() > DataHeaderFlyweight.HEADER_LENGTH)
                {
                    assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                    assertThat(buffer.position(), is(ALIGNED_FRAME_LENGTH));
                    receivedDataFrames.incrementAndGet();
                    return true;
                }

                return false;
            });

        assertThat(receivedDataFrames.get(), greaterThanOrEqualTo(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleNak() throws Exception
    {
        EventLogger.logInvocation();

        // let buffers get connected

        // this will not be sent yet
        while (!publication.offer(payload, 0, PAYLOAD.length))
        {
            Thread.yield();
        }

        final AtomicInteger termId = new AtomicInteger();
        final AtomicInteger receivedZeroLengthData = new AtomicInteger();
        final AtomicInteger receivedDataFrames = new AtomicInteger();

        // should only see 0 length data until SM is sent
        DatagramTestHelper.receiveUntil(receiverChannel,
            (buffer) ->
            {
                dataHeader.wrap(buffer, 0);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
                termId.set(dataHeader.termId());
                receivedZeroLengthData.incrementAndGet();
                return true;
            });

        assertThat(receivedZeroLengthData.get(), greaterThanOrEqualTo(1));

        // send SM
        sendSM(termId.get());

        // assert the received Data Frames are correct
        DatagramTestHelper.receiveUntil(receiverChannel,
            (buffer) ->
            {
                dataHeader.wrap(buffer, 0);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));

                if (dataHeader.frameLength() > DataHeaderFlyweight.HEADER_LENGTH)
                {
                    assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                    receivedDataFrames.incrementAndGet();
                    return true;
                }
                return false;
            });

        assertThat(receivedDataFrames.get(), greaterThanOrEqualTo(1));

        // send NAK
        sendNak(termId.get(), 0, ALIGNED_FRAME_LENGTH);

        // assert the received Data Frames are correct
        DatagramTestHelper.receiveUntil(receiverChannel,
            (buffer) ->
            {
                dataHeader.wrap(buffer, 0);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                assertThat(dataHeader.channelId(), is(CHANNEL_ID));
                assertThat(dataHeader.sessionId(), is(SESSION_ID));
                assertThat(dataHeader.termId(), is(termId.get()));

                if (dataHeader.frameLength() > DataHeaderFlyweight.HEADER_LENGTH)
                {
                    assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                    assertThat(buffer.position(), is(ALIGNED_FRAME_LENGTH));
                    receivedDataFrames.incrementAndGet();
                    return true;
                }

                return false;
            });

        assertThat(receivedDataFrames.get(), greaterThanOrEqualTo(2));
    }

    private void sendSM(final int termId) throws Exception
    {
        final ByteBuffer smBuffer = ByteBuffer.allocate(StatusMessageFlyweight.HEADER_LENGTH);
        statusMessage.wrap(new AtomicBuffer(smBuffer), 0);

        statusMessage.receiverWindowSize(1000)
                     .highestContiguousTermOffset(0)
                     .termId(termId)
                     .channelId(CHANNEL_ID)
                     .sessionId(SESSION_ID)
                     .frameLength(StatusMessageFlyweight.HEADER_LENGTH)
                     .headerType(HeaderFlyweight.HDR_TYPE_SM)
                     .flags((short) 0)
                     .version(HeaderFlyweight.CURRENT_VERSION);

        smBuffer.position(0);
        smBuffer.limit(StatusMessageFlyweight.HEADER_LENGTH);
        final int bytesSent = receiverChannel.send(smBuffer, srcAddress);

        assertThat(bytesSent, is(StatusMessageFlyweight.HEADER_LENGTH));
    }

    private void sendNak(final int termId, final int termOffset, final int length) throws Exception
    {
        final ByteBuffer nakBuffer = ByteBuffer.allocate(NakFlyweight.HEADER_LENGTH);
        nakHeader.wrap(new AtomicBuffer(nakBuffer), 0);

        nakHeader.length(length)
                 .termOffset(termOffset)
                 .termId(termId)
                 .channelId(CHANNEL_ID)
                 .sessionId(SESSION_ID)
                 .frameLength(NakFlyweight.HEADER_LENGTH)
                 .headerType(HeaderFlyweight.HDR_TYPE_NAK)
                 .flags((short) 0)
                 .version(HeaderFlyweight.CURRENT_VERSION);

        nakBuffer.position(0);
        nakBuffer.limit(NakFlyweight.HEADER_LENGTH);
        final int bytesSent = receiverChannel.send(nakBuffer, srcAddress);

        assertThat(bytesSent, is(NakFlyweight.HEADER_LENGTH));
    }
}
