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
package uk.co.real_logic.aeron.examples;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.BusySpinIdleStrategy;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.driver.MediaDriver;

/**
 * Pong component of Ping-Pong.
 *
 * Echoes back messages
 */
public class Pong
{
    private static final int PING_STREAM_ID = ExampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = ExampleConfiguration.PONG_STREAM_ID;
    private static final String PING_CHANNEL = ExampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = ExampleConfiguration.PONG_CHANNEL;
    private static final int FRAME_COUNT_LIMIT = ExampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = ExampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    private static final BusySpinIdleStrategy PING_HANDLER_IDLER = new BusySpinIdleStrategy();

    public static void main(final String[] args) throws Exception
    {
        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;

        final Aeron.Context ctx = new Aeron.Context();
        final BusySpinIdleStrategy idleStrategy = new BusySpinIdleStrategy();

        System.out.println("Subscribing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
        System.out.println("Publishing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);

        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication pongPublication = aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID);
             final Subscription pingSubscription = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID,
                 (buffer, offset, length, sessionId, flags) ->
                     pingHandler(pongPublication, buffer, offset, length, sessionId, flags)))
        {
            while (true)
            {
                final int workCount = pingSubscription.poll(FRAME_COUNT_LIMIT);
                idleStrategy.idle(workCount);
            }
        }

//        CloseHelper.quietClose(driver);
    }

    public static void pingHandler(
        final Publication pongPublication,
        final AtomicBuffer buffer,
        final int offset,
        final int length,
        final int sessionId,
        final byte flags)
    {
        while (!pongPublication.offer(buffer, offset, length))
        {
            PING_HANDLER_IDLER.idle(0);
        }
    }
}
