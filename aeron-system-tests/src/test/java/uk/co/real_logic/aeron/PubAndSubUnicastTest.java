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

import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.Subscriber.DataHandler;
import static uk.co.real_logic.aeron.Subscriber.NewSourceEventHandler;
import static uk.co.real_logic.aeron.util.CommonContext.DIRS_DELETE_ON_EXIT_PROP_NAME;

/**
 * Test that has a publisher and subscriber and single media driver for unicast cases
 */
public class PubAndSubUnicastTest
{
    private static final Destination DESTINATION = new Destination("udp://localhost:54321");
    private static final long CHANNEL_ID = 1L;
    private static final long SESSION_ID = 2L;

    private Aeron producingClient;
    private Aeron receivingClient;
    private MediaDriver driver;
    private Subscriber subscriber;
    private Source source;

    @Before
    public void setupClients() throws Exception
    {
        System.setProperty(DIRS_DELETE_ON_EXIT_PROP_NAME, "true");
        driver = new MediaDriver();

        final DataHandler dataHandler = mock(DataHandler.class);
        final NewSourceEventHandler sourceHandler = mock(NewSourceEventHandler.class);

        producingClient = Aeron.newSingleMediaDriver(newAeronContext());
        receivingClient = Aeron.newSingleMediaDriver(newAeronContext());

        subscriber = receivingClient.newSubscriber(new Subscriber.Context()
                                                       .destination(DESTINATION)
                                                       .channel(CHANNEL_ID, dataHandler)
                                                       .newSourceEvent(sourceHandler));

        source = producingClient.newSource(new Source.Context().destination(DESTINATION)
                                                               .sessionId(SESSION_ID));

    }

    private Aeron.ClientContext newAeronContext()
    {
        return new Aeron.ClientContext();
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

    private void doWork(final int times) throws Exception
    {
        for (int i = 0; i < times; i++)
        {
            doWork();
        }
    }

    private void doWork() throws Exception
    {
        producingClient.conductor().doWork();
        driver.conductor().doWork();
        driver.sender().doWork();
        driver.receiver().doWork();
        receivingClient.conductor().doWork();
        subscriber.read();
    }

    @Test
    public void clientsSpinUp() throws Exception
    {
        doWork(3);
    }

    @Test
    @Ignore("isn't finished yet")
    public void publisherMessagesSubscriber() throws Exception
    {
        doWork(5);
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
