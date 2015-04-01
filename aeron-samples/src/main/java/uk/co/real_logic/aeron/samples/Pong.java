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
package uk.co.real_logic.aeron.samples;

import uk.co.real_logic.aeron.*;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.BusySpinIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pong component of Ping-Pong.
 *
 * Echoes back messages
 */
public class Pong
{
    private static final int PING_STREAM_ID = SampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = SampleConfiguration.PONG_STREAM_ID;
    private static final String PING_CHANNEL = SampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = SampleConfiguration.PONG_CHANNEL;
    private static final int FRAME_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    private static final BusySpinIdleStrategy PING_HANDLER_IDLE_STRATEGY = new BusySpinIdleStrategy();

    public static void main(final String[] args) throws Exception
    {
        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;

        final Aeron.Context ctx = new Aeron.Context();
        final BusySpinIdleStrategy idleStrategy = new BusySpinIdleStrategy();

        System.out.println("Subscribing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
        System.out.println("Publishing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication pongPublication = aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID);
             final Subscription pingSubscription = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID,
                 new FragmentAssemblyAdapter(
                     (buffer, offset, length, header) -> pingHandler(pongPublication, buffer, offset, length))))
        {
            while (running.get())
            {
                final int fragmentsRead = pingSubscription.poll(FRAME_COUNT_LIMIT);
                idleStrategy.idle(fragmentsRead);
            }

            System.out.println("Shutting down...");
        }

        CloseHelper.quietClose(driver);
    }

    public static void pingHandler(
        final Publication pongPublication, final DirectBuffer buffer, final int offset, final int length)
    {
        while (!pongPublication.offer(buffer, offset, length))
        {
            PING_HANDLER_IDLE_STRATEGY.idle(0);
        }
    }
}
