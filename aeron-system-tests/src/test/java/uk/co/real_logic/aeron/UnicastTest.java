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
import uk.co.real_logic.aeron.util.ConductorBuffers;
import uk.co.real_logic.aeron.util.CountersResource;
import uk.co.real_logic.aeron.util.SharedDirectories;

import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.Subscriber.DataHandler;
import static uk.co.real_logic.aeron.Subscriber.NewSourceEventHandler;
import static uk.co.real_logic.aeron.util.CommonConfiguration.ADMIN_DIR;

public class UnicastTest
{
    private static final Destination DESTINATION = new Destination("udp://localhost:54321");
    private static final long CHANNEL_ID = 1L;
    private static final long SESSION_ID = 2L;

    @ClassRule
    public static CountersResource counters = new CountersResource();

    @ClassRule
    public static SharedDirectories dataDir = new SharedDirectories();

    @ClassRule
    public static ConductorBuffers buffers = new ConductorBuffers(ADMIN_DIR);

    private DataHandler dataHandler;
    private NewSourceEventHandler sourceHandler;

    private Aeron producingClient;
    private Aeron receivingClient;
    private MediaDriver driver;
    private Subscriber subscriber;
    private Source source;

    @Before
    public void setupClients() throws Exception
    {
        driver = new MediaDriver();

        dataHandler = mock(DataHandler.class);
        sourceHandler = mock(NewSourceEventHandler.class);

        producingClient = Aeron.newSingleMediaDriver(new Aeron.Context());
        receivingClient = Aeron.newSingleMediaDriver(new Aeron.Context());

        subscriber = receivingClient.newSubscriber(new Subscriber.Context()
                                                       .destination(DESTINATION)
                                                       .channel(CHANNEL_ID, dataHandler)
                                                       .newSourceEvent(sourceHandler));

        source = producingClient.newSource(new Source.Context().destination(DESTINATION)
                                                               .sessionId(SESSION_ID));
    }

    @After
    public void closeEverything() throws Exception
    {
        subscriber.close();
        producingClient.close();
        receivingClient.close();
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
        driver.adminThread().process();
        driver.senderThread().process();
        driver.receiverThread().process();
        receivingClient.conductor().process();
        subscriber.read();
    }

    @Test
    public void clientsSpinUp() throws Exception
    {
        process(3);
    }

    @Ignore
    @Test
    public void publisherMessagesSubscriber()
    {
        // TODO: simple message send/read
    }

    @Ignore
    @Test
    public void messagesShouldContinueAfterBufferRollover()
    {
        // TODO: send enough data to rollover a buffer
    }

    @Ignore
    @Test
    public void receiverShouldNakMissingData()
    {
        // TODO: throw away some packets, check they are delivered
    }

}
