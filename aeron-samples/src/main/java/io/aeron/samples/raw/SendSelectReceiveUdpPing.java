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
import org.agrona.concurrent.SigInt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;

import static java.nio.channels.SelectionKey.OP_READ;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see HackSelectReceiveSendUdpPong
 */
public class SendSelectReceiveUdpPing
{
    private static final InetSocketAddress SEND_ADDRESS = new InetSocketAddress("localhost", Common.PING_PORT);

    private int sequenceNumber;

    public static void main(final String[] args) throws IOException
    {
        new SendSelectReceiveUdpPing().run();
    }

    private void run() throws IOException
    {
        final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        final ByteBuffer buffer = ByteBuffer.allocateDirect(Configuration.MTU_LENGTH_DEFAULT);

        final DatagramChannel receiveChannel = DatagramChannel.open();
        Common.init(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", Common.PONG_PORT));

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
                    final long timestampNs = buffer.getLong(SIZE_OF_LONG);

                    if (receivedSequenceNumber != sequenceNumber)
                    {
                        throw new IllegalStateException(
                            "data Loss:" + sequenceNumber + " to " + receivedSequenceNumber);
                    }

                    final long durationNs = System.nanoTime() - timestampNs;
                    histogram.recordValue(durationNs);
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

        while (running.get())
        {
            measureRoundTrip(histogram, SEND_ADDRESS, buffer, sendChannel, selector, running);

            histogram.reset();
            System.gc();
            LockSupport.parkNanos(1000 * 1000 * 1000);
        }
    }

    private void measureRoundTrip(
        final Histogram histogram,
        final InetSocketAddress sendAddress,
        final ByteBuffer buffer,
        final DatagramChannel sendChannel,
        final Selector selector,
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

        histogram.outputPercentileDistribution(System.out, 1000.0);
    }
}
