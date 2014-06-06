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
import uk.co.real_logic.aeron.Channel;
import uk.co.real_logic.aeron.Destination;
import uk.co.real_logic.aeron.Source;
import uk.co.real_logic.aeron.mediadriver.MediaDriver;
import uk.co.real_logic.aeron.util.BitUtil;
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Example Aeron pusblisher application
 */
public class ExamplePublisher
{
    public static final int CHANNEL_ID = 10;
    public static final Destination DESTINATION = new Destination("udp://localhost:40123");

    private static final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocateDirect(256));

    public static void main(final String[] args)
    {
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final Aeron.Context context = new Aeron.Context().errorHandler(ExamplePublisher::onError);

        try (final MediaDriver driver = ExampleUtil.createEmbeddedMediaDriver(executor);
             final Aeron aeron = ExampleUtil.createAeron(context, executor);
             final Source source = aeron.newSource(DESTINATION);
             final Channel channel = source.newChannel(CHANNEL_ID))
        {

            for (int i = 0; i < 10; i++)
            {
                buffer.putString(BitUtil.SIZE_OF_BYTE, "Hello World!", ByteOrder.LITTLE_ENDIAN);
                buffer.putByte(0, (byte)i);

                System.out.print("offering " + i);
                final boolean result = channel.offer(buffer, 0, 14);

                if (false == result)
                {
                    System.out.println(" ah?!");
                }
                else
                {
                    System.out.println(" yay!");
                }

                Thread.sleep(1000);
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }

        executor.shutdown();
    }

    public static void onError(final String destination,
                               final long sessionId,
                               final long channelId,
                               final String message,
                               final HeaderFlyweight cause)
    {
        System.err.println(message);
    }
}
