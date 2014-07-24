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

import uk.co.real_logic.aeron.driver.MediaDriver;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.function.Function;

public class DatagramTestHelper
{
    public static void receiveUntil(final DatagramChannel channel, final Function<ByteBuffer, Boolean> handler)
    {
        final ByteBuffer buffer = ByteBuffer.allocate(MediaDriver.READ_BYTE_BUFFER_SZ);
        boolean done = false;

        while (!done)
        {
            try
            {
                buffer.clear();
                final InetSocketAddress address = (InetSocketAddress)channel.receive(buffer);

                if (null != address)
                {
                    done = handler.apply(buffer);
                }
                else
                {
                    Thread.yield();
                }
            }
            catch (final Exception ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }
}
