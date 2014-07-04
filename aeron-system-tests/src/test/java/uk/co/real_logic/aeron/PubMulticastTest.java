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
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.aeron.mediadriver.MediaDriver;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.concurrent.logbuffer.FrameDescriptor;
import uk.co.real_logic.aeron.util.event.EventLogger;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.NakFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static uk.co.real_logic.aeron.util.CommonContext.DIRS_DELETE_ON_EXIT_PROP_NAME;

/**
 * Test that has a publisher and single media driver for multicast cases. Uses socket as receiver/consumer endpoint.
 */
public class PubMulticastTest
{
    public static final EventLogger LOGGER = new EventLogger(PubMulticastTest.class);

    private static final String DATA_ADDRESS = "224.20.30.39";
    private static final String CONTROL_ADDRESS = "224.20.30.40";
    private static final int DST_PORT = 54321;
    private static final String DESTINATION = "udp://localhost@" + DATA_ADDRESS + ":" + DST_PORT;
    private static final long CHANNEL_ID = 1L;
    private static final long SESSION_ID = 2L;
    private static final byte[] PAYLOAD = "Payload goes here!".getBytes();

    private static final int COUNTER_BUFFER_SZ = 1024;

    private final AtomicBuffer counterValuesBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);
    private final AtomicBuffer counterLabelsBuffer = new AtomicBuffer(new byte[COUNTER_BUFFER_SZ]);
    private final AtomicBuffer payload = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));

    private final InetSocketAddress controlAddress = new InetSocketAddress(CONTROL_ADDRESS, DST_PORT);

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
        System.setProperty(DIRS_DELETE_ON_EXIT_PROP_NAME, "true");

        final NetworkInterface ifc = NetworkInterface.getByInetAddress(InetAddress.getByName("localhost"));
        receiverChannel = DatagramChannel.open();
        receiverChannel.configureBlocking(false);
        receiverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        receiverChannel.bind(new InetSocketAddress(DST_PORT));
        receiverChannel.join(InetAddress.getByName(DATA_ADDRESS), ifc);
        receiverChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, ifc);

        driver = new MediaDriver();

        producingClient = Aeron.newSingleMediaDriver(newAeronContext());

        payload.putBytes(0, PAYLOAD);

        executorService = Executors.newSingleThreadExecutor();

        driver.invokeEmbedded();
        producingClient.invoke(executorService);

        publication = producingClient.addPublication(DESTINATION, CHANNEL_ID, SESSION_ID);
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
        // TODO: publication.release();

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
        LOGGER.logInvocation();

        // let buffers get connected

        // this will not be sent yet
        while (!publication.offer(payload, 0, PAYLOAD.length))
        {
            Thread.sleep(10);
        }

        // sleep so we are sure some 0 length data has been sent
        Thread.sleep(500);

        final ByteBuffer rcvBuffer = ByteBuffer.allocate(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length);
        long termId = 0, rcvedZeroLengthData = 0, rcvedDataFrames = 0;

        // should only see 0 length data until SM is sent
        while (receiverChannel.receive(rcvBuffer) != null)
        {
            dataHeader.wrap(rcvBuffer, 0);
            assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
            assertThat(dataHeader.channelId(), is(CHANNEL_ID));
            assertThat(dataHeader.sessionId(), is(SESSION_ID));
            termId = dataHeader.termId();

            assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
            assertThat(rcvBuffer.position(), is(DataHeaderFlyweight.HEADER_LENGTH));
            rcvedZeroLengthData++;
            rcvBuffer.clear();
        }

        assertThat(rcvedZeroLengthData, greaterThanOrEqualTo(1L));

        // send SM
        sendSM(termId);

        // sleep to make sure that the sender thread in the media driver has a chance to send data
        Thread.sleep(100);

        // assert the received Data Frames are correctly formed. Either SM or real data
        while (receiverChannel.receive(rcvBuffer) != null)
        {
            dataHeader.wrap(rcvBuffer, 0);
            assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
            assertThat(dataHeader.channelId(), is(CHANNEL_ID));
            assertThat(dataHeader.sessionId(), is(SESSION_ID));
            assertThat(dataHeader.termId(), is(termId));

            if (dataHeader.frameLength() > DataHeaderFlyweight.HEADER_LENGTH)
            {
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                assertThat(rcvBuffer.position(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                rcvedDataFrames++;
            }
            else
            {
                rcvedZeroLengthData++;
            }

            rcvBuffer.clear();
        }

        assertThat(rcvedDataFrames, is(1L));
    }

    @Test(timeout = 1000)
    public void shouldHandleNak() throws Exception
    {
        LOGGER.logInvocation();

        // let buffers get connected

        // this will not be sent yet
        while (!publication.offer(payload, 0, PAYLOAD.length))
        {
            Thread.sleep(10);
        }

        // sleep so we are sure some 0 length data has been sent
        Thread.sleep(500);

        final ByteBuffer rcvBuffer = ByteBuffer.allocate(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length);
        long termId = 0, rcvedZeroLengthData = 0, rcvedDataFrames = 0;

        // should only see 0 length data until SM is sent
        while (receiverChannel.receive(rcvBuffer) != null)
        {
            dataHeader.wrap(rcvBuffer, 0);
            assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
            assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
            termId = dataHeader.termId();
            rcvedZeroLengthData++;
            rcvBuffer.clear();
        }

        assertThat(rcvedZeroLengthData, greaterThanOrEqualTo(1L));

        // send SM
        sendSM(termId);

        // sleep to make sure that the sender thread in the media driver has a chance to send data
        Thread.sleep(100);

        // assert the received Data Frames are correct
        while (receiverChannel.receive(rcvBuffer) != null)
        {
            dataHeader.wrap(rcvBuffer, 0);
            assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));

            if (dataHeader.frameLength() > DataHeaderFlyweight.HEADER_LENGTH)
            {
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                assertThat(rcvBuffer.position(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                rcvedDataFrames++;
            }
            else
            {
                rcvedZeroLengthData++;
            }

            rcvBuffer.clear();
        }

        assertThat(rcvedDataFrames, greaterThanOrEqualTo(1L));

        // send NAK
        sendNak(termId, 0, FrameDescriptor.FRAME_ALIGNMENT);

        // sleep to make sure that the sender thread in the media driver has a chance to send data
        Thread.sleep(100);

        // assert the received Data Frames are correct
        while (receiverChannel.receive(rcvBuffer) != null)
        {
            dataHeader.wrap(rcvBuffer, 0);
            assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
            assertThat(dataHeader.channelId(), is(CHANNEL_ID));
            assertThat(dataHeader.sessionId(), is(SESSION_ID));
            assertThat(dataHeader.termId(), is(termId));

            if (dataHeader.frameLength() > DataHeaderFlyweight.HEADER_LENGTH)
            {
                assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                assertThat(rcvBuffer.position(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
                rcvedDataFrames++;
            }
            else
            {
                rcvedZeroLengthData++;
            }

            rcvBuffer.clear();
        }

        assertThat(rcvedDataFrames, greaterThanOrEqualTo(2L));
    }

    @Test(timeout = 1000)
    @Ignore("isn't finished yet - send enough data to rollover a buffer")
    public void messagesShouldContinueAfterBufferRollover()
    {
        LOGGER.logInvocation();
    }

    private void sendSM(final long termId) throws Exception
    {
        final ByteBuffer smBuffer = ByteBuffer.allocate(StatusMessageFlyweight.HEADER_LENGTH);
        statusMessage.wrap(new AtomicBuffer(smBuffer), 0);

        statusMessage.receiverWindow(1000)
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
        final int bytesSent = receiverChannel.send(smBuffer, controlAddress);

        assertThat(bytesSent, is(StatusMessageFlyweight.HEADER_LENGTH));
    }

    private void sendNak(final long termId, final long termOffset, final long length) throws Exception
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
        final int bytesSent = receiverChannel.send(nakBuffer, controlAddress);

        assertThat(bytesSent, is(NakFlyweight.HEADER_LENGTH));
    }
}
