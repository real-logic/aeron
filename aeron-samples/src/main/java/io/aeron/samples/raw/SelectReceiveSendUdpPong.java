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
package io.aeron.samples.raw;

import io.aeron.driver.Configuration;
import org.agrona.concurrent.SigInt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;

import static java.nio.channels.SelectionKey.OP_READ;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see SendHackSelectReceiveUdpPing
 */
public class SelectReceiveSendUdpPong
{
    public static void main(final String[] args) throws IOException
    {
        new SelectReceiveSendUdpPong().run();
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

        final IntSupplier handler =
            () ->
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

        receiveChannel.register(selector, OP_READ, handler);

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
            }

            final Set<SelectionKey> selectedKeys = selector.selectedKeys();
            final Iterator<SelectionKey> iter = selectedKeys.iterator();

            while (iter.hasNext())
            {
                final SelectionKey key = iter.next();
                if (key.isReadable())
                {
                    ((IntSupplier)key.attachment()).getAsInt();
                }

                iter.remove();
            }
        }
    }
}
