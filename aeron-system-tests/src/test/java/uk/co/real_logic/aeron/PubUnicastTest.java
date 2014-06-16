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
import uk.co.real_logic.aeron.util.ConductorShmBuffers;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static uk.co.real_logic.aeron.util.CommonConfiguration.ADMIN_DIR_NAME;
import static uk.co.real_logic.aeron.util.CommonConfiguration.DIRS_DELETE_ON_EXIT_PROP_NAME;

/**
 * Test that has a publisher and single media driver for unicast cases. Uses socket as receiver/consumer endpoint.
 */
public class PubUnicastTest
{
    private static final String HOST = "localhost";
    private static final int PORT = 54321;
    private static final int SRC_PORT = 54322;
    private static final Destination DESTINATION = new Destination("udp://" + HOST + ":" + SRC_PORT +
                                                                        "@" + HOST + ":" + PORT);
    private static final long CHANNEL_ID = 1L;
    private static final long SESSION_ID = 2L;
    private static final byte[] PAYLOAD = "Payload goes here!".getBytes();

    private final AtomicBuffer payload = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));

    private final InetSocketAddress srcAddr = new InetSocketAddress(HOST, SRC_PORT);

    private Aeron producingClient;
    private MediaDriver driver;
    private Source source;
    private Channel channel;

    private DatagramChannel receiverChannel;

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

    private ExecutorService executorService;

    @Before
    public void setupClientAndMediaDriver() throws Exception
    {
        System.setProperty(DIRS_DELETE_ON_EXIT_PROP_NAME, "true");

        receiverChannel = DatagramChannel.open();
        receiverChannel.configureBlocking(false);
        receiverChannel.bind(new InetSocketAddress(HOST, PORT));

        driver = new MediaDriver();

        producingClient = Aeron.newSingleMediaDriver(newAeronContext());

        source = producingClient.newSource(new Source.Context().destination(DESTINATION)
                .sessionId(SESSION_ID));

        channel = source.newChannel(CHANNEL_ID);

        payload.putBytes(0, PAYLOAD);

        executorService = Executors.newFixedThreadPool(4);

        driver.invoke(executorService);
        producingClient.invoke(executorService);
    }

    private Aeron.Context newAeronContext()
    {
        return new Aeron.Context().conductorShmBuffers(new ConductorShmBuffers(ADMIN_DIR_NAME));
    }

    @After
    public void closeEverything() throws Exception
    {
        producingClient.stop();
        driver.stop();

        receiverChannel.close();
        source.close();
        producingClient.close();
        driver.close();
        executorService.shutdown();
    }

    @Test
    public void shouldSendCorrectlyFormedSingleDataFrames() throws Exception
    {
        // let buffers get connected

        // this will not be sent yet
        while (!channel.offer(payload, 0, PAYLOAD.length))
        {
            Thread.sleep(10);
        }

        // sleep so we are sure some 0 length data has been sent
        Thread.sleep(500);

        final ByteBuffer rcvBuffer = ByteBuffer.allocate(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length);
        rcvBuffer.clear();
        final AtomicBuffer aBuffer = new AtomicBuffer(rcvBuffer);
        long termId = 0;
        int rcvedZeroLengthData = 0, rcvedDataFrames = 0;

        // should only see 0 length data until SM is sent
        InetSocketAddress addr;
        while((addr = (InetSocketAddress)receiverChannel.receive(rcvBuffer)) != null)
        {
            dataHeader.wrap(aBuffer, 0);
            assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
            assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH));
            assertThat(dataHeader.channelId(), is(CHANNEL_ID));
            assertThat(dataHeader.sessionId(), is(SESSION_ID));
            assertThat(rcvBuffer.position(), is(DataHeaderFlyweight.HEADER_LENGTH));
            termId = dataHeader.termId();
            assertThat(addr, is(srcAddr));
            rcvedZeroLengthData++;
            rcvedDataFrames++;
            rcvBuffer.clear();
        }

        assertThat(rcvedZeroLengthData, greaterThanOrEqualTo(1));

        // send SM
        sendSM(termId);

        // sleep to make sure that the sender thread in the media driver has a chance to send data
        Thread.sleep(100);

        // assert the received Data Frames are correctly formed
        rcvBuffer.clear();
        while((addr = (InetSocketAddress)receiverChannel.receive(rcvBuffer)) != null)
        {
            dataHeader.wrap(aBuffer, 0);
            assertThat(dataHeader.headerType(), is(HeaderFlyweight.HDR_TYPE_DATA));
            assertThat(dataHeader.frameLength(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
            assertThat(dataHeader.channelId(), is(CHANNEL_ID));
            assertThat(dataHeader.sessionId(), is(SESSION_ID));
            assertThat(rcvBuffer.position(), is(DataHeaderFlyweight.HEADER_LENGTH + PAYLOAD.length));
            assertThat(dataHeader.termId(), is(termId));
            assertThat(addr, is(srcAddr));
            rcvedDataFrames++;
            rcvBuffer.clear();
        }

        assertThat(rcvedDataFrames - rcvedZeroLengthData, greaterThanOrEqualTo(1));
    }

    @Test
    @Ignore("isn't finished yet")
    public void publisherMessagesSubscriber() throws Exception
    {
        // TODO: simple message send/read
    }

    @Test
    @Ignore("isn't finished yet")
    public void messagesShouldContinueAfterBufferRollover()
    {
        // TODO: send enough data to rollover a buffer
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
            .flags((short)0)
            .version(HeaderFlyweight.CURRENT_VERSION);

        smBuffer.position(0);
        smBuffer.limit(StatusMessageFlyweight.HEADER_LENGTH);
        final int byteSent = receiverChannel.send(smBuffer, srcAddr);

        assertThat(byteSent, is(StatusMessageFlyweight.HEADER_LENGTH));
    }
}
