/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import org.HdrHistogram.Histogram;
import org.agrona.nio.NioSelectedKeySet;
import org.agrona.concurrent.SigInt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToIntFunction;

import static java.nio.channels.SelectionKey.OP_READ;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see HackSelectReceiveSendUdpPong
 */
public class SendHackSelectReceiveUdpPing implements ToIntFunction<SelectionKey>
{
    private static final InetSocketAddress SEND_ADDRESS = new InetSocketAddress("localhost", Common.PING_PORT);

    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(Configuration.MTU_LENGTH_DEFAULT);
    private DatagramChannel receiveChannel;
    private int sequenceNumber;

    public static void main(final String[] args) throws IOException
    {
        new SendHackSelectReceiveUdpPing().run();
    }

    private void run() throws IOException
    {
        receiveChannel = DatagramChannel.open();
        Common.init(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", Common.PONG_PORT));

        final DatagramChannel sendChannel = DatagramChannel.open();
        Common.init(sendChannel);

        final Selector selector = Selector.open();
        receiveChannel.register(selector, OP_READ, this);
        final NioSelectedKeySet keySet = Common.keySet(selector);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (running.get())
        {
            measureRoundTrip(HISTOGRAM, SEND_ADDRESS, buffer, sendChannel, selector, keySet, running);

            HISTOGRAM.reset();
            System.gc();
            LockSupport.parkNanos(1000 * 1000 * 1000);
        }
    }

    public int applyAsInt(final SelectionKey key)
    {
        try
        {
            buffer.clear();
            receiveChannel.receive(buffer);

            final long receivedSequenceNumber = buffer.getLong(0);
            final long timestamp = buffer.getLong(SIZE_OF_LONG);

            if (receivedSequenceNumber != sequenceNumber)
            {
                throw new IllegalStateException("Data Loss:" + sequenceNumber + " to " + receivedSequenceNumber);
            }

            final long duration = System.nanoTime() - timestamp;
            HISTOGRAM.recordValue(duration);
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }

        return 1;
    }

    private void measureRoundTrip(
        final Histogram histogram,
        final InetSocketAddress sendAddress,
        final ByteBuffer buffer,
        final DatagramChannel sendChannel,
        final Selector selector,
        final NioSelectedKeySet keySet,
        final AtomicBoolean running)
        throws IOException
    {
        for (sequenceNumber = 0; sequenceNumber < Common.NUM_MESSAGES; sequenceNumber++)
        {
            final long timestamp = System.nanoTime();

            buffer.clear();
            buffer.putLong(sequenceNumber);
            buffer.putLong(timestamp);
            buffer.flip();

            sendChannel.send(buffer, sendAddress);

            while (selector.selectNow() == 0)
            {
                if (!running.get())
                {
                    return;
                }
            }

            keySet.forEach(this);
        }

        histogram.outputPercentileDistribution(System.out, 1000.0);
    }
}
