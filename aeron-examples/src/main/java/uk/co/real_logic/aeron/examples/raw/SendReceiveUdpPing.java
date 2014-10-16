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

import org.HdrHistogram.Histogram;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.SigInt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;
import static uk.co.real_logic.aeron.examples.raw.Common.setup;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see ReceiveSendUdpPong
 */
public class SendReceiveUdpPing
{
    public static void main(final String[] args) throws IOException
    {
        new SendReceiveUdpPing().run();
    }

    private void run() throws IOException
    {
        final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        final InetSocketAddress sendAddress = new InetSocketAddress("localhost", Common.PING_PORT);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(MTU_LENGTH_DEFAULT);

        final DatagramChannel receiveChannel = DatagramChannel.open();
        setup(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", Common.PONG_PORT));

        final DatagramChannel sendChannel = DatagramChannel.open();
        setup(sendChannel);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (running.get())
        {
            measureRoundTrip(histogram, sendAddress, buffer, receiveChannel, sendChannel, running);

            histogram.reset();
            System.gc();
            LockSupport.parkNanos(1000 * 1000 * 1000);
        }
    }

    private void measureRoundTrip(
        final Histogram histogram,
        final InetSocketAddress sendAddress,
        final ByteBuffer buffer,
        final DatagramChannel receiveChannel,
        final DatagramChannel sendChannel,
        final AtomicBoolean running)
        throws IOException
    {
        for (int sequenceNumber = 0; sequenceNumber < Common.NUM_MESSAGES; sequenceNumber++)
        {
            final long timestamp = System.nanoTime();

            buffer.clear();
            buffer.putLong(sequenceNumber);
            buffer.putLong(timestamp);
            buffer.flip();

            sendChannel.send(buffer, sendAddress);

            buffer.clear();
            while (receiveChannel.receive(buffer) == null)
            {
                if (!running.get())
                {
                    return;
                }
            }

            final long receivedSequenceNumber = buffer.getLong(0);
            if (receivedSequenceNumber != sequenceNumber)
            {
                throw new IllegalStateException("Data Loss:" + sequenceNumber + " to " + receivedSequenceNumber);
            }

            final long duration = System.nanoTime() - timestamp;
            histogram.recordValue(duration);
        }

        histogram.outputPercentileDistribution(System.out, 1000.0);
    }
}
