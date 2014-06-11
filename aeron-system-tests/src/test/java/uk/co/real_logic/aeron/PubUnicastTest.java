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
import uk.co.real_logic.aeron.mediadriver.MediaDriver;
import uk.co.real_logic.aeron.util.ConductorShmBuffers;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.aeron.util.CommonConfiguration.ADMIN_DIR_NAME;
import static uk.co.real_logic.aeron.util.CommonConfiguration.DIRS_DELETE_ON_EXIT_PROP_NAME;

/**
 * Test that has a publisher and single media driver for unicast cases. Uses socket as receiver/consumer endpoint.
 */
public class PubUnicastTest
{
    private static final String HOST = "localhost";
    private static final int PORT = 54321;
    private static final Destination DESTINATION = new Destination("udp://" + HOST + ":" + PORT);
    private static final long CHANNEL_ID = 1L;
    private static final long SESSION_ID = 2L;
    private static final byte[] PAYLOAD = "Payload goes here!".getBytes();

    private final AtomicBuffer payload = new AtomicBuffer(ByteBuffer.allocate(PAYLOAD.length));

    private Aeron producingClient;
    private MediaDriver driver;
    private Source source;
    private Channel channel;

    private DatagramChannel receiverChannel;

    private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
    private final StatusMessageFlyweight statusMessage = new StatusMessageFlyweight();

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
    }

    private Aeron.Context newAeronContext()
    {
        return new Aeron.Context().conductorShmBuffers(new ConductorShmBuffers(ADMIN_DIR_NAME));
    }

    @After
    public void closeEverything() throws Exception
    {
        receiverChannel.close();
        producingClient.close();
        source.close();
        driver.close();
        driver.conductor().nioSelector().selectNowWithNoProcessing();
    }

    private void process(final int times) throws Exception
    {
        for (int i = 0; i < times; i++)
        {
            process();
        }
    }

    private void process() throws Exception
    {
        producingClient.conductor().process();
        driver.conductor().process();
        driver.sender().process();
        driver.receiver().process();
    }

    @Test
    public void shouldSpinUpClientAndMediaDriver() throws Exception
    {
        process(3);
    }

    // TODO: get rid of these tests. Think they are redundant with good tests for media driver and client
    // instead rely on PubAndSubUnicastTest for systems integration once we have broadcast buffer in

    @Test
    @Ignore("isn't finished yet")
    public void shouldSendCorrectDataFrames() throws Exception
    {
        // let buffers get connected
        process(1);

        assertTrue(channel.offer(payload, 0, PAYLOAD.length));

        process(1);
        // TODO: process to get buffers setup from setUp()
        // TODO: assert receiving a correct 0 length data frame on senderChannel
        // TODO: send SM to get client to send
        // TODO: assert the received Data Frames are correctly formed
    }

    @Test
    @Ignore("isn't finished yet")
    public void publisherMessagesSubscriber() throws Exception
    {
        process(5);
        // TODO: simple message send/read
    }

    @Test
    @Ignore("isn't finished yet")
    public void messagesShouldContinueAfterBufferRollover()
    {
        // TODO: send enough data to rollover a buffer
    }

}
