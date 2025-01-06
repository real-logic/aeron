/*
 * Copyright 2014-2025 Real Logic Limited.
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
import org.HdrHistogram.Histogram;
import org.agrona.BitUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.HighResolutionTimer;
import org.agrona.concurrent.SigInt;
import org.agrona.hints.ThreadHints;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.samples.raw.Common.init;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see ReceiveSendUdpPong
 */
public class BurstSendReceiveUdpPing
{
    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws IOException if an error occurs with the channel.
     */
    public static void main(final String[] args) throws IOException
    {
        String remoteHost = "localhost";
        if (1 <= args.length)
        {
            remoteHost = args[0];
        }

        int packetSize = 16;
        if (2 <= args.length)
        {
            packetSize = min(Configuration.MTU_LENGTH_DEFAULT, max(packetSize, Integer.parseInt(args[1])));
        }

        int burstSize = 1;
        if (3 <= args.length)
        {
            burstSize = min(1024, Integer.parseInt(args[2]));
        }

        if (SystemUtil.isWindows())
        {
            HighResolutionTimer.enable();
        }

        System.out.printf("Remote host: %s, packet size: %d, burstSize: %d%n", remoteHost, packetSize, burstSize);

        final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);

        final ByteBuffer buffer = ByteBuffer.allocateDirect(Configuration.MTU_LENGTH_DEFAULT);
        for (int i = 0, length = buffer.capacity(); i < length; i++)
        {
            buffer.put(i, (byte)0xFF);
        }

        final DatagramChannel receiveChannel = DatagramChannel.open();
        receiveChannel.bind(new InetSocketAddress("0.0.0.0", Common.PONG_PORT));

        final InetSocketAddress sendAddress = new InetSocketAddress(remoteHost, Common.PING_PORT);
        final DatagramChannel sendChannel = DatagramChannel.open();
        init(sendChannel);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (running.get())
        {
            measureRoundTrip(
                histogram, sendAddress, buffer, packetSize, burstSize, receiveChannel, sendChannel, running);

            histogram.reset();
            System.gc();
            LockSupport.parkNanos(1_000_000_000L);
        }
    }

    private static void measureRoundTrip(
        final Histogram histogram,
        final InetSocketAddress sendAddress,
        final ByteBuffer buffer,
        final int packetSize,
        final int burstSize,
        final DatagramChannel receiveChannel,
        final DatagramChannel sendChannel,
        final AtomicBoolean running)
        throws IOException
    {
        for (int sequenceNumber = 0; sequenceNumber < Common.NUM_MESSAGES; sequenceNumber += burstSize)
        {
            for (int i = 0; i < burstSize; i++)
            {
                final long timestampNs = System.nanoTime();

                buffer.clear();
                buffer.putLong(sequenceNumber + i);
                buffer.putLong(timestampNs);
                buffer.position(packetSize);
                buffer.flip();

                sendChannel.send(buffer, sendAddress);
            }

            for (int i = 0; i < burstSize; i++)
            {
                buffer.clear();
                while (running.get())
                {
                    if (null != receiveChannel.receive(buffer))
                    {
                        break;
                    }
                    ThreadHints.onSpinWait();
                }

                final long receivedSequenceNumber = buffer.getLong(0);
                if (receivedSequenceNumber != sequenceNumber + i)
                {
                    throw new IllegalStateException("Data Loss: " + sequenceNumber + " to " + receivedSequenceNumber);
                }

                final long durationNs = System.nanoTime() - buffer.getLong(BitUtil.SIZE_OF_LONG);
                histogram.recordValue(durationNs);
            }
        }

        histogram.outputPercentileDistribution(System.out, 1000.0);
    }
}
