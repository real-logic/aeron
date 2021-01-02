/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.samples.raw;

import io.aeron.driver.Configuration;
import org.agrona.hints.ThreadHints;
import org.agrona.nio.NioSelectedKeySet;
import org.agrona.concurrent.SigInt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToIntFunction;

import static java.nio.channels.SelectionKey.OP_READ;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see SendHackSelectReceiveUdpPing
 */
public class HackSelectReceiveSendUdpPong
{
    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws IOException if an error occurs with the channel.
     */
    public static void main(final String[] args) throws IOException
    {
        new HackSelectReceiveSendUdpPong().run();
    }

    private void run() throws IOException
    {
        final InetSocketAddress sendAddress = new InetSocketAddress("localhost", Common.PONG_PORT);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Configuration.MTU_LENGTH_DEFAULT);

        final DatagramChannel receiveChannel = DatagramChannel.open();
        Common.init(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", Common.PING_PORT));

        final DatagramChannel sendChannel = DatagramChannel.open();
        Common.init(sendChannel);

        final Selector selector = Selector.open();
        final NioSelectedKeySet keySet = Common.keySet(selector);

        final ToIntFunction<SelectionKey> handler =
            (key) ->
            {
                try
                {
                    buffer.clear();
                    receiveChannel.receive(buffer);

                    final long receivedSequenceNumber = buffer.getLong(0);
                    final long receivedTimestamp = buffer.getLong(SIZE_OF_LONG);

                    buffer.clear();
                    buffer.putLong(receivedSequenceNumber);
                    buffer.putLong(receivedTimestamp);
                    buffer.flip();

                    sendChannel.send(buffer, sendAddress);
                }
                catch (final IOException ex)
                {
                    ex.printStackTrace();
                }

                return 1;
            };

        receiveChannel.register(selector, OP_READ, null);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (true)
        {
            while (selector.selectNow() == 0)
            {
                if (!running.get())
                {
                    return;
                }
                ThreadHints.onSpinWait();
            }

            keySet.forEach(handler);
        }
    }
}
