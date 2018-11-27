/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.junit.After;
import org.junit.Test;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.BitUtil;
import org.agrona.LangUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test that a second subscriber can be stopped and started again while data is being published.
 */
public class StopStartSecondSubscriberTest
{
    public static final String CHANNEL1 = "aeron:udp?endpoint=localhost:54325";
    public static final String CHANNEL2 = "aeron:udp?endpoint=localhost:54326";
    private static final int STREAM_ID1 = 1;
    private static final int STREAM_ID2 = 2;

    private MediaDriver driverOne;
    private MediaDriver driverTwo;
    private Aeron publisherOne;
    private Aeron subscriberOne;
    private Aeron publisherTwo;
    private Aeron subscriberTwo;
    private Subscription subscriptionOne;
    private Publication publicationOne;
    private Subscription subscriptionTwo;
    private Publication publicationTwo;

    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    private final MutableInteger subOneCount = new MutableInteger();
    private final FragmentHandler fragmentHandlerOne = (buffer, offset, length, header) -> subOneCount.value++;
    private final MutableInteger subTwoCount = new MutableInteger();
    private final FragmentHandler fragmentHandlerTwo = (buffer, offset, length, header) -> subTwoCount.value++;

    private void launch(final String channelOne, final int streamOne, final String channelTwo, final int streamTwo)
    {
        driverOne = MediaDriver.launchEmbedded(
            new MediaDriver.Context()
                .errorHandler(Throwable::printStackTrace)
                .termBufferSparseFile(true));

        driverTwo = MediaDriver.launchEmbedded(
            new MediaDriver.Context()
                .errorHandler(Throwable::printStackTrace)
                .termBufferSparseFile(true));

        publisherOne = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverOne.aeronDirectoryName()));
        subscriberOne = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverTwo.aeronDirectoryName()));
        publisherTwo = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverOne.aeronDirectoryName()));
        subscriberTwo = Aeron.connect(new Aeron.Context().aeronDirectoryName(driverTwo.aeronDirectoryName()));

        subscriptionOne = subscriberOne.addSubscription(channelOne, streamOne);
        subscriptionTwo = subscriberTwo.addSubscription(channelTwo, streamTwo);
        publicationOne = publisherOne.addPublication(channelOne, streamOne);
        publicationTwo = publisherTwo.addPublication(channelTwo, streamTwo);
    }

    @After
    public void after()
    {
        subscriberOne.close();
        publisherOne.close();
        subscriberTwo.close();
        publisherTwo.close();

        driverOne.close();
        driverTwo.close();

        driverOne.context().deleteAeronDirectory();
        driverTwo.context().deleteAeronDirectory();
    }

    @Test(timeout = 10_000)
    public void shouldSpinUpAndShutdown()
    {
        launch(CHANNEL1, STREAM_ID1, CHANNEL2, STREAM_ID2);
    }

    @Test(timeout = 10000)
    public void shouldReceivePublishedMessage()
    {
        launch(CHANNEL1, STREAM_ID1, CHANNEL2, STREAM_ID2);

        buffer.putInt(0, 1);

        final int messagesPerPublication = 1;

        while (publicationOne.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        while (publicationTwo.offer(buffer, 0, BitUtil.SIZE_OF_INT) < 0L)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        final MutableInteger fragmentsRead1 = new MutableInteger();
        final MutableInteger fragmentsRead2 = new MutableInteger();
        SystemTest.executeUntil(
            () -> fragmentsRead1.get() >= messagesPerPublication && fragmentsRead2.get() >= messagesPerPublication,
            (i) ->
            {
                fragmentsRead1.value += subscriptionOne.poll(fragmentHandlerOne, 10);
                fragmentsRead2.value += subscriptionTwo.poll(fragmentHandlerTwo, 10);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(9900));

        assertEquals(messagesPerPublication, subOneCount.get());
        assertEquals(messagesPerPublication, subTwoCount.get());
    }

    @Test(timeout = 10_000)
    public void shouldReceiveMessagesAfterStopStartOnSameChannelSameStream()
    {
        shouldReceiveMessagesAfterStopStart(CHANNEL1, STREAM_ID1, CHANNEL1, STREAM_ID1);
    }

    @Test(timeout = 10_000)
    public void shouldReceiveMessagesAfterStopStartOnSameChannelDifferentStreams()
    {
        shouldReceiveMessagesAfterStopStart(CHANNEL1, STREAM_ID1, CHANNEL1, STREAM_ID2);
    }

    @Test(timeout = 10_000)
    public void shouldReceiveMessagesAfterStopStartOnDifferentChannelsSameStream()
    {
        shouldReceiveMessagesAfterStopStart(CHANNEL1, STREAM_ID1, CHANNEL2, STREAM_ID1);
    }

    @Test(timeout = 10_000)
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
        final String channelOne, final int streamOne, final String channelTwo, final int streamTwo)
    {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final int numMessages = 1;
        final MutableInteger subscriber2AfterRestartCount = new MutableInteger();
        final AtomicBoolean running = new AtomicBoolean(true);

        final FragmentHandler fragmentHandler2b =
            (buffer, offset, length, header) -> subscriber2AfterRestartCount.value++;

        launch(channelOne, streamOne, channelTwo, streamTwo);

        buffer.putInt(0, 1);

        executor.execute(() -> doPublisherWork(publicationOne, running));
        executor.execute(() -> doPublisherWork(publicationTwo, running));

        final MutableInteger fragmentsReadOne = new MutableInteger();
        final MutableInteger fragmentsReadTwo = new MutableInteger();
        final BooleanSupplier fragmentsReadCondition =
            () -> fragmentsReadOne.get() >= numMessages && fragmentsReadTwo.get() >= numMessages;

        SystemTest.executeUntil(
            fragmentsReadCondition,
            (i) ->
            {
                fragmentsReadOne.value += subscriptionOne.poll(fragmentHandlerOne, 1);
                fragmentsReadTwo.value += subscriptionTwo.poll(fragmentHandlerTwo, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(4900));

        assertTrue(subOneCount.get() >= numMessages);
        assertTrue(subTwoCount.get() >= numMessages);

        subscriptionTwo.close();

        fragmentsReadOne.set(0);
        fragmentsReadTwo.set(0);

        subscriptionTwo = subscriberTwo.addSubscription(channelTwo, streamTwo);

        SystemTest.executeUntil(
            fragmentsReadCondition,
            (i) ->
            {
                fragmentsReadOne.value += subscriptionOne.poll(fragmentHandlerOne, 1);
                fragmentsReadTwo.value += subscriptionTwo.poll(fragmentHandler2b, 1);
                Thread.yield();
            },
            Integer.MAX_VALUE,
            TimeUnit.MILLISECONDS.toNanos(4900));

        running.set(false);

        assertTrue("Expecting subscriberOne to receive messages the entire time",
            subOneCount.get() >= numMessages * 2);
        assertTrue("Expecting subscriberTwo to receive messages before being stopped and started",
            subTwoCount.get() >= numMessages);
        assertTrue("Expecting subscriberTwo to receive messages after being stopped and started",
            subscriber2AfterRestartCount.get() >= numMessages);

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
