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
package uk.co.real_logic.aeron.examples.raw;

import uk.co.real_logic.aeron.common.concurrent.SigInt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;
import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see SendReceiveUdpPing
 */
public class ReceiveSendUdpPong
{
    public static void main(final String[] args) throws IOException
    {
        new ReceiveSendUdpPong().run();
    }

    private void run() throws IOException
    {
        final InetSocketAddress sendAddress = new InetSocketAddress("localhost", Common.PONG_PORT);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(MTU_LENGTH_DEFAULT);
        final DatagramChannel receiveChannel = DatagramChannel.open();

        Common.setup(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", Common.PING_PORT));

        final DatagramChannel sendChannel = DatagramChannel.open();
        Common.setup(sendChannel);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (true)
        {
            buffer.clear();
            while (receiveChannel.receive(buffer) == null)
            {
                if (!running.get())
                {
                    return;
                }
            }

            final long receivedSequenceNumber = buffer.getLong(0);
            final long receivedTimestamp = buffer.getLong(SIZE_OF_LONG);

            buffer.clear();
            buffer.putLong(receivedSequenceNumber);
            buffer.putLong(receivedTimestamp);
            buffer.flip();

            sendChannel.send(buffer, sendAddress);
        }
    }
}
