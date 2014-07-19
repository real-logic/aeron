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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;

import static uk.co.real_logic.aeron.util.CommonContext.DIRS_DELETE_ON_EXIT_PROP_NAME;

/**
 * Test that has a publisher and subscriber and single media driver for unicast and multicast cases
 */
@RunWith(Theories.class)
public class PubAndSubTest
{
    @DataPoint
    public static final String UNICAST_DESTINATION = "udp://localhost:54325";

    @DataPoint
    public static final String MULTICAST_DESTINATION = "udp://localhost@224.20.30.39:54326";

    private static final long CHANNEL_ID = 1L;
    private static final long SESSION_ID = 2L;

    private Aeron publishingClient;
    private Aeron subscribingClient;
    private MediaDriver driver;
    private Subscription subscription;
    private Publication publication;

    private ExecutorService executorService;

    private void setup(final String destination) throws Exception
    {
        System.setProperty(DIRS_DELETE_ON_EXIT_PROP_NAME, "true");

        executorService = Executors.newFixedThreadPool(2);

        final MediaDriver.MediaDriverContext ctx = new MediaDriver.MediaDriverContext();

        ctx.warnIfDirectoriesExist(false);

        driver = new MediaDriver(ctx);

        final DataHandler dataHandler = mock(DataHandler.class);

        publishingClient = Aeron.newSingleMediaDriver(new Aeron.ClientContext());
        subscribingClient = Aeron.newSingleMediaDriver(new Aeron.ClientContext());

        driver.invokeEmbedded();
        publishingClient.invoke(executorService);
        subscribingClient.invoke(executorService);

        publication = publishingClient.addPublication(destination, CHANNEL_ID, SESSION_ID);
        subscription = subscribingClient.addSubscription(destination, CHANNEL_ID, dataHandler);
    }

    @After
    public void closeEverything() throws Exception
    {
        publication.release();

        subscribingClient.shutdown();
        publishingClient.shutdown();
        driver.shutdown();

        subscription.release();
        subscribingClient.close();
        publishingClient.close();
        driver.close();
        executorService.shutdown();
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldSpinUpAndShutdown(final String destination) throws Exception
    {
        setup(destination);

        Thread.sleep(100);
    }

    @Theory
    @Ignore("isn't finished yet - simple message send/receive")
    public void shouldReceivePublishedMessage(final String destination) throws Exception
    {
        setup(destination);

        Thread.sleep(100);
    }

    @Theory
    @Ignore("isn't finished yet = send enough data to rollover a buffer")
    public void shouldContinueAfterBufferRollover(final String destination) throws Exception
    {
        setup(destination);

        Thread.sleep(100);
    }
}
