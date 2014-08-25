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
import uk.co.real_logic.aeron.common.protocol.*;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static uk.co.real_logic.aeron.common.BitUtil.align;

/**
 * Test that has a publisher and single media driver for multicast cases. Uses socket as receiver/consumer endpoint.
 */
public class PubMulticastTest
{
    private static final String DATA_ADDRESS = "224.20.30.39";
    private static final String CONTROL_ADDRESS = "224.20.30.40";
    private static final int DST_PORT = 54321;
    private static final String URI = "udp://localhost@" + DATA_ADDRESS + ":" + DST_PORT;
    private static final int STREAM_ID = 1;
    private static final int SESSION_ID = 2;
    private static final byte[] PAYLOAD = "Payload goes here!".getBytes();
    private static final int ALIGNED_FRAME_LENGTH =
        align(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length, FrameDescriptor.FRAME_ALIGNMENT);

    private final AtomicBuffer payload = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));

    private final InetSocketAddress controlAddress = new InetSocketAddress(CONTROL_ADDRESS, DST_PORT);

    private Aeron producingClient;
    private MediaDriver driver;
    private Publication publication;

    private DatagramChannel receiverChannel;

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();
    private final NakFlyweight nakHeader = new NakFlyweight();
    private final SetupFlyweight setupHeader = new SetupFlyweight();

    @Before
    public void setupClientAndMediaDriver() throws Exception
    {
        payload.putBytes(0, PAYLOAD);

        final NetworkInterface ifc = NetworkInterface.getByInetAddress(InetAddress.getByName("localhost"));
        receiverChannel = DatagramChannel.open();
        receiverChannel.configureBlocking(false);
        receiverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        receiverChannel.bind(new InetSocketAddress(DST_PORT));
        receiverChannel.join(InetAddress.getByName(DATA_ADDRESS), ifc);
        receiverChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, ifc);

        final MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.dirsDeleteOnExit(true);

        driver = MediaDriver.launch(ctx);
        producingClient = Aeron.connect(newAeronContext());
        publication = producingClient.addPublication(URI, STREAM_ID, SESSION_ID);
    }

    private Aeron.Context newAeronContext()
    {
        return new Aeron.Context();
    }

    @After
    public void closeEverything() throws Exception
    {
        if (null != publication)
        {
            publication.release();
        }

        producingClient.close();
        driver.close();

        if (null != receiverChannel)
        {
            receiverChannel.close();
        }
    }

    @Test(timeout = 1000)
    public void shouldSendCorrectlyFormedSingleDataFrames() throws Exception
    {
        // let buffers get connected

        // this will not be sent yet
        while (!publication.offer(payload, 0, PAYLOAD.length))
        {
            Thread.yield();
        }

        final AtomicInteger termId = new AtomicInteger();
        final AtomicInteger receivedSetupFrames = new AtomicInteger();

        // should only see SETUP until SM is sent
        DatagramTestHelper.receiveUntil(receiverChannel,
            (buffer) ->
            {
                setupHeader.wrap(buffer, 0);
                if (setupHeader.headerType() == HeaderFlyweight.HDR_TYPE_SETUP)
                {
                    assertThat(setupHeader.streamId(), is(STREAM_ID));
                    assertThat(setupHeader.sessionId(), is(SESSION_ID));
                    termId.set(setupHeader.termId());
                    assertThat(setupHeader.frameLength(), is(SetupFlyweight.HEADER_LENGTH));
                    assertThat(buffer.position(), is(SetupFlyweight.HEADER_LENGTH));
                    receivedSetupFrames.incrementAndGet();

                    return true;
                }

                return false;
            });

        assertThat(receivedSetupFrames.get(), greaterThanOrEqualTo(1));

        // send SM
        sendSM(termId.get());

        final AtomicInteger receivedDataFrames = new AtomicInteger();

        // assert the received Data Frames are correctly formed. Either SM or real data
        DatagramTestHelper.receiveUntil(receiverChannel,
            (buffer) ->
            {
                dataHeader.wrap(buffer, 0);
                assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
                assertThat(dataHeader.streamId(), is(STREAM_ID));
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

        assertThat(receivedDataFrames.get(), is(1));
    }

    @Test(timeout = 1000)
    public void shouldHandleNak() throws Exception
    {
        // let buffers get connected

        // this will not be sent yet
        while (!publication.offer(payload, 0, PAYLOAD.length))
        {
            Thread.yield();
        }

        final AtomicInteger termId = new AtomicInteger();
        final AtomicInteger receivedSetupFrames = new AtomicInteger();
        final AtomicInteger receivedDataFrames = new AtomicInteger();

        // should only see SETUP until SM is sent
        DatagramTestHelper.receiveUntil(receiverChannel,
            (buffer) ->
            {
                setupHeader.wrap(buffer, 0);
                if (setupHeader.headerType() == HeaderFlyweight.HDR_TYPE_SETUP &&
                    setupHeader.frameLength() == SetupFlyweight.HEADER_LENGTH)
                {
                    termId.set(setupHeader.termId());
                    receivedSetupFrames.incrementAndGet();

                    return true;
                }

                return false;
            });

        assertThat(receivedSetupFrames.get(), greaterThanOrEqualTo(1));

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
                assertThat(dataHeader.streamId(), is(STREAM_ID));
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
                     .streamId(STREAM_ID)
                     .sessionId(SESSION_ID)
                     .frameLength(StatusMessageFlyweight.HEADER_LENGTH)
                     .headerType(HeaderFlyweight.HDR_TYPE_SM)
                     .flags((short)0)
                     .version(HeaderFlyweight.CURRENT_VERSION);

        smBuffer.position(0);
        smBuffer.limit(StatusMessageFlyweight.HEADER_LENGTH);
        final int bytesSent = receiverChannel.send(smBuffer, controlAddress);

        assertThat(bytesSent, is(StatusMessageFlyweight.HEADER_LENGTH));
    }

    private void sendNak(final int termId, final int termOffset, final int length) throws Exception
    {
        final ByteBuffer nakBuffer = ByteBuffer.allocate(NakFlyweight.HEADER_LENGTH);
        nakHeader.wrap(new AtomicBuffer(nakBuffer), 0);

        nakHeader.length(length)
                 .termOffset(termOffset)
                 .termId(termId)
                 .streamId(STREAM_ID)
                 .sessionId(SESSION_ID)
                 .frameLength(NakFlyweight.HEADER_LENGTH)
                 .headerType(HeaderFlyweight.HDR_TYPE_NAK)
                 .flags((short)0)
                 .version(HeaderFlyweight.CURRENT_VERSION);

        nakBuffer.position(0);
        nakBuffer.limit(NakFlyweight.HEADER_LENGTH);
        final int bytesSent = receiverChannel.send(nakBuffer, controlAddress);

        assertThat(bytesSent, is(NakFlyweight.HEADER_LENGTH));
    }
}
