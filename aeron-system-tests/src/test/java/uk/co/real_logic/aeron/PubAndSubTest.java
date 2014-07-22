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
import org.junit.experimental.theories.*;
import org.junit.runner.RunWith;
import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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

    private AtomicBuffer buffer = new AtomicBuffer(new byte[256]);
    private DataHandler dataHandler = mock(DataHandler.class);

    private ExecutorService executorService;

    private void setup(final String destination) throws Exception
    {
        executorService = Executors.newFixedThreadPool(2);

        final MediaDriver.DriverContext ctx = new MediaDriver.DriverContext();

        ctx.dirsDeleteOnExit(true);
        ctx.warnIfDirectoriesExist(false);

        driver = new MediaDriver(ctx);

        publishingClient = Aeron.newClient(new Aeron.ClientContext());
        subscribingClient = Aeron.newClient(new Aeron.ClientContext());

        driver.invokeEmbedded();
        publishingClient.invoke(executorService);
        subscribingClient.invoke(executorService);

        publication = publishingClient.addPublication(destination, CHANNEL_ID, SESSION_ID);
        subscription = subscribingClient.addSubscription(destination, CHANNEL_ID, dataHandler);
    }

    @After
    public void closeEverything() throws Exception
    {
        System.err.println("closeEverything");
        publication.release();
        subscription.release();

        subscribingClient.shutdown();
        publishingClient.shutdown();
        driver.shutdown();

        subscribingClient.close();
        publishingClient.close();
        driver.close();
        executorService.shutdown();
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldSpinUpAndShutdown(final String destination) throws Exception
    {
        EventLogger.logInvocation();

        setup(destination);

        Thread.sleep(100);
    }

    @Theory
    @Test(timeout = 1000)
    public void shouldReceivePublishedMessage(final String destination) throws Exception
    {
        EventLogger.logInvocation();

        setup(destination);

        buffer.putInt(0, 1);

        publication.offer(buffer, 0, BitUtil.SIZE_OF_INT);

        int msgs[] = new int[1];
        SystemTestHelper.executeUntil(() -> (msgs[0] > 0),
            (i) ->
            {
                msgs[0] += subscription.poll(10);
                Thread.yield();
            },
            Integer.MAX_VALUE, TimeUnit.MILLISECONDS.toNanos(500));

        verify(dataHandler).onData(anyObject(), eq(DataHeaderFlyweight.HEADER_LENGTH), eq(BitUtil.SIZE_OF_INT),
                eq(SESSION_ID), eq((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS));
    }

    @Theory
    @Test(timeout = 1000)
    @Ignore("isn't finished yet = send enough data to rollover a buffer")
    public void shouldContinueAfterBufferRollover(final String destination) throws Exception
    {
        setup(destination);

        Thread.sleep(100);
    }
}
