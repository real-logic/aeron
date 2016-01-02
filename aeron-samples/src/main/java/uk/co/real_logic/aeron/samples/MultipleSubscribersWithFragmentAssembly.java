/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.samples;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssembler;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.SigInt;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A subscriber application with two subscriptions which can receive fragmented messages
 * Creates two subscriptions on a given channel subscribed to two different stream IDs.
 * The default STREAM_ID and CHANNEL are configured in {@link SampleConfiguration}. The default
 * channel and stream IDs can be changed by setting Java system properties at the command line, e.g.:
 * -Daeron.sample.channel=udp://localhost:5555 -Daeron.sample.streamId=20
 */
public class MultipleSubscribersWithFragmentAssembly
{
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int STREAM_ID_1 = SampleConfiguration.STREAM_ID;
    private static final int STREAM_ID_2 = SampleConfiguration.STREAM_ID + 1;

    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    public static void main(final String[] args) throws Exception
    {
        System.out.format("Subscribing to %s on stream ID %d and stream ID %d%n",
            CHANNEL, STREAM_ID_1, STREAM_ID_2);

        final Aeron.Context ctx = new Aeron.Context()
            .availableImageHandler(MultipleSubscribersWithFragmentAssembly::eventAvailableImage)
            .unavailableImageHandler(MultipleSubscribersWithFragmentAssembly::eventUnavailableImage);

        final FragmentAssembler dataHandler1 = new FragmentAssembler(reassembledStringMessage1(STREAM_ID_1));
        final FragmentAssembler dataHandler2 = new FragmentAssembler(reassembledStringMessage2(STREAM_ID_2));

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        try (final Aeron aeron = Aeron.connect(ctx);
             final Subscription subscription1 = aeron.addSubscription(CHANNEL, STREAM_ID_1);
             final Subscription subscription2 = aeron.addSubscription(CHANNEL, STREAM_ID_2))
        {
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

            int idleCount = 0;

            while (running.get())
            {
                final int fragmentsRead1 = subscription1.poll(dataHandler1, FRAGMENT_COUNT_LIMIT);
                final int fragmentsRead2 = subscription2.poll(dataHandler2, FRAGMENT_COUNT_LIMIT);

                if ((fragmentsRead1 + fragmentsRead2) == 0)
                {
                    idleStrategy.idle(idleCount++);
                }
                else
                {
                    idleCount = 0;
                }
            }

            System.out.println("Shutting down...");
        }
    }

    /**
     * Print the information for an available image to stdout.
     *
     * @param image that has been created
     */
    public static void eventAvailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        System.out.format(
            "new image on %s streamId %x sessionId %x from %s%n",
            subscription.channel(), subscription.streamId(), image.sessionId(), image.sourceIdentity());
    }

    /**
     * This handler is called when image is unavailable
     *
     * @param image that has gone inactive
     */
    public static void eventUnavailableImage(final Image image)
    {
        final Subscription subscription = image.subscription();
        System.out.format(
            "inactive image on %s streamId %d sessionId %x%n",
            subscription.channel(), subscription.streamId(), image.sessionId());
    }

    /**
     * Return a reusable, parameterized {@link FragmentHandler} that prints to stdout for the first stream(STREAM)
     *
     * @param streamId to show when printing
     * @return subscription data handler function that prints the message contents
     * @throws Exception if an error occurs
     */
    public static FragmentHandler reassembledStringMessage1(final int streamId) throws Exception
    {
        return
            (buffer, offset, length, header) ->
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);

                System.out.format(
                    "message to stream %d from session %x term id %x term offset %d (%d@%d)%n",
                    streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset);

                if (length != 10000)
                {
                    System.out.format(
                        "Received message was not assembled properly; received length was %d, but was expecting 10000%n",
                        length);
                }
            };
    }

    /**
     * Return a reusable, parameterized {@link FragmentHandler} that prints to stdout for the second stream (STREAM + 1)
     *
     * @param streamId to show when printing
     * @return subscription data handler function that prints the message contents
     * @throws Exception if an error occurs
     */
    public static FragmentHandler reassembledStringMessage2(final int streamId) throws Exception
    {
        return
            (buffer, offset, length, header) ->
            {
                final byte[] data = new byte[length];
                buffer.getBytes(offset, data);

                System.out.format(
                    "message to stream %d from session %x term id %x term offset %d (%d@%d)%n",
                    streamId, header.sessionId(), header.termId(), header.termOffset(), length, offset);

                if (length != 9000)
                {
                    System.out.format(
                        "Received message was not assembled properly; received length was %d, but was expecting 9000%n",
                        length);
                }
            };
    }
}
