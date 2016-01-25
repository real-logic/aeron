/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
import org.junit.Test;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.MutableInteger;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that a second subscriber can be stopped and started again while data is being published.
 */
public class StopStartSecondSubscriberTest
{
    public static final String CHANNEL1 = "aeron:udp?remote=localhost:54325";
    public static final String CHANNEL2 = "aeron:udp?remote=localhost:54326";
    private static final int STREAM_ID1 = 1;
    private static final int STREAM_ID2 = 2;

    private MediaDriver driver1;
    private MediaDriver driver2;
    private Aeron publishingClient1;
    private Aeron subscribingClient1;
    private Aeron publishingClient2;
    private Aeron subscribingClient2;
    private Subscription subscription1;
    private Publication publication1;
    private Subscription subscription2;
    private Publication publication2;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[8192]);
    private int subscriber1Count = 0;
    private FragmentHandler fragmentHandler1 = (buffer, offset, length, header) -> subscriber1Count++;
    private int subscriber2Count = 0;
    private FragmentHandler fragmentHandler2 = (buffer, offset, length, header) -> subscriber2Count++;

    final MediaDriver.Context mediaDriverContext1 = new MediaDriver.Context();
    final MediaDriver.Context mediaDriverContext2 = new MediaDriver.Context();

    private void launch(final String channel1, final int stream1, final String channel2, final int stream2)
    {
        driver1 = MediaDriver.launchEmbedded(mediaDriverContext1);
        driver2 = MediaDriver.launchEmbedded(mediaDriverContext2);

        final Aeron.Context publishingAeronContext1 = new Aeron.Context();
        final Aeron.Context subscribingAeronContext1 = new Aeron.Context();
        final Aeron.Context publishingAeronContext2 = new Aeron.Context();
        final Aeron.Context subscribingAeronContext2 = new Aeron.Context();

        publishingAeronContext1.aeronDirectoryName(driver1.aeronDirectoryName());
        publishingAeronContext2.aeronDirectoryName(driver1.aeronDirectoryName());
        subscribingAeronContext1.aeronDirectoryName(driver2.aeronDirectoryName());
        subscribingAeronContext2.aeronDirectoryName(driver2.aeronDirectoryName());

        publishingClient1 = Aeron.connect(publishingAeronContext1);
        subscribingClient1 = Aeron.connect(subscribingAeronContext1);
        publishingClient2 = Aeron.connect(publishingAeronContext2);
        subscribingClient2 = Aeron.connect(subscribingAeronContext2);

        publication1 = publishingClient1.addPublication(channel1, stream1);
        subscription1 = subscribingClient1.addSubscription(channel1, stream1);
        publication2 = publishingClient2.addPublication(channel2, stream2);
        subscription2 = subscribingClient2.addSubscription(channel2, stream2);
    }

    @After
    public void closeEverything()
    {
        subscribingClient1.close();
        publishingClient1.close();
        subscribingClient2.close();
        publishingClient2.close();

        driver1.close();
        driver2.close();

        mediaDriverContext1.deleteAeronDirectory();
        mediaDriverContext2.deleteAeronDirectory();
    }

    @Test(timeout = 10000)
    public void shouldSpinUpAndShutdown()
    {
        launch(CHANNEL1, STREAM_ID1, CHANNEL2, STREAM_ID2);
    }

    @Test(timeout = 10000)
    public void shouldReceivePublishedMessage()
    {
        launch(CHANNEL1, STREAM_ID1, CHANNEL2, STREAM_ID2);

        buffer.putInt(0, 1);

        final int numMessagesPerPublication = 1;

        while (publication1.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Thread.yield();
        }

        while (publication2.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            Thread.yield();
        }

        final int fragmentsRead[] = new int[2];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] >= numMessagesPerPublication && fragmentsRead[1] >= numMessagesPerPublication,
            (i) ->
            {
                fragmentsRead[0] += subscription1.poll(fragmentHandler1, 10);
                fragmentsRead[1] += subscription2.poll(fragmentHandler2, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(9900));

        assertEquals(numMessagesPerPublication, subscriber1Count);
        assertEquals(numMessagesPerPublication, subscriber2Count);
    }

    @Test(timeout = 10000)
    public void shouldReceiveMessagesAfterStopStartOnSameChannelSameStream()
    {
        shouldReceiveMessagesAfterStopStart(CHANNEL1, STREAM_ID1, CHANNEL1, STREAM_ID1);
    }

    @Test(timeout = 10000)
    public void shouldReceiveMessagesAfterStopStartOnSameChannelDifferentStreams()
    {
        shouldReceiveMessagesAfterStopStart(CHANNEL1, STREAM_ID1, CHANNEL1, STREAM_ID2);
    }

    @Test(timeout = 10000)
    public void shouldReceiveMessagesAfterStopStartOnDifferentChannelsSameStream()
    {
        shouldReceiveMessagesAfterStopStart(CHANNEL1, STREAM_ID1, CHANNEL2, STREAM_ID1);
    }

    @Test(timeout = 10000)
    public void shouldReceiveMessagesAfterStopStartOnDifferentChannelsDifferentStreams()
    {
        shouldReceiveMessagesAfterStopStart(CHANNEL1, STREAM_ID1, CHANNEL2, STREAM_ID2);
    }

    private void doPublisherWork(final Publication publication, final AtomicBoolean running)
    {
        while (running.get())
        {
            while (running.get() && publication.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
            {
                Thread.yield();
            }
        }
    }

    private void shouldReceiveMessagesAfterStopStart(
        final String channel1, final int stream1, final String channel2, final int stream2)
    {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final int numMessages = 1;
        final MutableInteger subscriber2AfterRestartCount = new MutableInteger();
        final AtomicBoolean running = new AtomicBoolean(true);

        final FragmentHandler fragmentHandler2b = (buffer, offset, length, header) -> subscriber2AfterRestartCount.value++;

        launch(channel1, stream1, channel2, stream2);

        buffer.putInt(0, 1);

        executor.execute(() -> doPublisherWork(publication1, running));
        executor.execute(() -> doPublisherWork(publication2, running));

        final int fragmentsRead[] = new int[2];
        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] >= numMessages && fragmentsRead[1] >= numMessages,
            (i) ->
            {
                fragmentsRead[0] += subscription1.poll(fragmentHandler1, 1);
                fragmentsRead[1] += subscription2.poll(fragmentHandler2, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(4900));

        assertTrue(subscriber1Count >= numMessages);
        assertTrue(subscriber2Count >= numMessages);

        // Stop the second subscriber
        subscription2.close();

        // Zero out the counters
        fragmentsRead[0] = 0;
        fragmentsRead[1] = 0;

        // Start the second subscriber again
        subscription2 = subscribingClient2.addSubscription(channel2, stream2);

        SystemTestHelper.executeUntil(
            () -> fragmentsRead[0] >= numMessages && fragmentsRead[1] >= numMessages,
            (i) ->
            {
                fragmentsRead[0] += subscription1.poll(fragmentHandler1, 1);
                fragmentsRead[1] += subscription2.poll(fragmentHandler2b, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(4900));

        running.set(false);

        assertTrue("Expecting subscriber1 to receive messages the entire time", subscriber1Count >= numMessages * 2);
        assertTrue("Expecting subscriber2 to receive messages before being stopped and started", subscriber2Count >= numMessages);
        assertTrue("Expecting subscriber2 to receive messages after being stopped and started",
            subscriber2AfterRestartCount.value >= numMessages);

        executor.shutdown();

        try
        {
            while (!executor.awaitTermination(1, TimeUnit.SECONDS))
            {
                System.err.println("Still awaiting termination");
            }
        }
        catch (final InterruptedException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
