/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigInt;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pong component of Ping-Pong.
 * <p>
 * Echoes back messages from {@link Ping}.
 *
 * @see Ping
 */
public class Pong
{
    private static final int PING_STREAM_ID = SampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = SampleConfiguration.PONG_STREAM_ID;
    private static final int FRAME_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final String PING_CHANNEL = SampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = SampleConfiguration.PONG_CHANNEL;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;
    private static final boolean EXCLUSIVE_PUBLICATIONS = SampleConfiguration.EXCLUSIVE_PUBLICATIONS;

    private static final IdleStrategy PING_HANDLER_IDLE_STRATEGY = new BusySpinIdleStrategy();

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded() : null;

        final Aeron.Context ctx = new Aeron.Context();
        ctx.availableImageHandler(SamplesUtil::printAvailableImage);
        ctx.unavailableImageHandler(SamplesUtil::printUnavailableImage);
        if (EMBEDDED_MEDIA_DRIVER)
        {
            ctx.aeronDirectoryName(driver.aeronDirectoryName());
        }

        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();

        System.out.println("Subscribing Ping at " + PING_CHANNEL + " on stream id " + PING_STREAM_ID);
        System.out.println("Publishing Pong at " + PONG_CHANNEL + " on stream id " + PONG_STREAM_ID);
        System.out.println("Using exclusive publications " + EXCLUSIVE_PUBLICATIONS);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        try (Aeron aeron = Aeron.connect(ctx);
            Subscription subscription = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID);
            Publication publication = EXCLUSIVE_PUBLICATIONS ?
                aeron.addExclusivePublication(PONG_CHANNEL, PONG_STREAM_ID) :
                aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID))
        {
            idleStrategy.reset();
            while (!subscription.isConnected())
            {
                idleStrategy.idle();
            }

            final Image image = subscription.imageAtIndex(0);
            final FragmentHandler fragmentHandler = new ImageFragmentAssembler((buffer, offset, length, header) ->
                pingHandler(publication, buffer, offset, length));

            while (running.get())
            {
                idleStrategy.idle(image.poll(fragmentHandler, FRAME_COUNT_LIMIT));
            }

            System.out.println("Shutting down...");
        }

        CloseHelper.close(driver);
    }

    private static void pingHandler(
        final Publication pongPublication,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        PING_HANDLER_IDLE_STRATEGY.reset();
        while (pongPublication.offer(buffer, offset, length) <= 0)
        {
            PING_HANDLER_IDLE_STRATEGY.idle();
        }
    }
}
