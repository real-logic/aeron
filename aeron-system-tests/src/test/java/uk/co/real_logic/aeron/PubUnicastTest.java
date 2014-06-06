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
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;

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

    @Test
    @Ignore("isn't finished yet")
    public void shouldSendCorrectDataFrames() throws Exception
    {
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
