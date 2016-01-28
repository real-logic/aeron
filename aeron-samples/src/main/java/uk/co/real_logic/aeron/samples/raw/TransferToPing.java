/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.samples.raw;

import org.HdrHistogram.Histogram;

import uk.co.real_logic.agrona.concurrent.SigInt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;
import static uk.co.real_logic.aeron.samples.raw.Common.init;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

public class TransferToPing
{
    private static final String LOCALHOST = "localhost";

    public static void main(final String[] args) throws IOException
    {
        final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);

        final FileChannel sendFileChannel = Common.createTmpFileChannel();
        final ByteBuffer sendByteBuffer = sendFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MTU_LENGTH_DEFAULT);
        final DatagramChannel sendDatagramChannel = DatagramChannel.open();
        init(sendDatagramChannel);
        sendDatagramChannel.bind(new InetSocketAddress(LOCALHOST, 40123));
        sendDatagramChannel.connect(new InetSocketAddress(LOCALHOST, 40124));

        final FileChannel receiveFileChannel = Common.createTmpFileChannel();
        final ByteBuffer receiveByteBuffer = receiveFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MTU_LENGTH_DEFAULT);
        final DatagramChannel receiveDatagramChannel = DatagramChannel.open();
        init(receiveDatagramChannel);
        receiveDatagramChannel.bind(new InetSocketAddress(LOCALHOST, 40126));
        receiveDatagramChannel.connect(new InetSocketAddress(LOCALHOST, 40125));

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (running.get())
        {
            measureRoundTrip(
                histogram,
                receiveFileChannel,
                receiveDatagramChannel,
                receiveByteBuffer,
                sendFileChannel,
                sendDatagramChannel,
                sendByteBuffer,
                running);

            histogram.reset();
            System.gc();
            LockSupport.parkNanos(1000 * 1000 * 1000);
        }
    }

    private static void measureRoundTrip(
        final Histogram histogram,
        final FileChannel receiveFileChannel,
        final DatagramChannel receiveDatagramChannel,
        final ByteBuffer receiveByteBuffer,
        final FileChannel sendFileChannel,
        final DatagramChannel sendDatagramChannel,
        final ByteBuffer sendByteBuffer,
        final AtomicBoolean running)
        throws IOException
    {
        final int packetSize = SIZE_OF_LONG * 2;

        for (int sequenceNumber = 0; sequenceNumber < Common.NUM_MESSAGES; sequenceNumber++)
        {
            final long timestamp = System.nanoTime();

            sendByteBuffer.putLong(0, sequenceNumber);
            sendByteBuffer.putLong(SIZE_OF_LONG, timestamp);

            final long bytesSent = sendFileChannel.transferTo(0, packetSize, sendDatagramChannel);
            if (packetSize != bytesSent)
            {
                throw new IllegalStateException("Invalid bytes sent " + bytesSent);
            }

            boolean available = false;
            while (!available)
            {
                if (!running.get())
                {
                    return;
                }

                final long bytesReceived = receiveFileChannel.transferFrom(receiveDatagramChannel, 0, packetSize);
                if (packetSize == bytesReceived)
                {
                    available = true;
                }
            }

            final long receivedSequenceNumber = receiveByteBuffer.getLong(0);
            if (receivedSequenceNumber != sequenceNumber)
            {
                throw new IllegalStateException("Data Loss:" + sequenceNumber + " to " + receivedSequenceNumber);
            }

            final long duration = System.nanoTime() - receiveByteBuffer.getLong(SIZE_OF_LONG);
            histogram.recordValue(duration);
        }

        histogram.outputPercentileDistribution(System.out, 1000.0);
    }
}
