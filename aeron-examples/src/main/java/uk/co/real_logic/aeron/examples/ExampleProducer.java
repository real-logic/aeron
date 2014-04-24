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
import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.nio.ByteBuffer;

/**
 * Example Aeron sender application
 */
public class ExampleProducer
{
    public static final int CHANNEL_ID = 10;
    public static final Destination DESTINATION = new Destination("udp://172.16.29.29:40123");

    public static void main(String[] args)
    {
        try
        {
            final Aeron.Builder builder = new Aeron.Builder().errorHandler(ExampleProducer::onError);
            final Aeron aeron = Aeron.newSingleMediaDriver(builder);
            final Source source = aeron.newSource(DESTINATION);
            final Channel aChannel = source.newChannel(CHANNEL_ID);
            final AtomicBuffer buffer = new AtomicBuffer(ByteBuffer.allocateDirect(256));

            // TODO: add data to buffer and get it ready to send
            aChannel.send(buffer);
            // TODO: wait around for something...
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
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
