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

import uk.co.real_logic.agrona.concurrent.SigInt;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;
import static uk.co.real_logic.aeron.samples.raw.Common.init;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_LONG;

public class TransferToPong
{
    private static final String LOCALHOST = "localhost";

    public static void main(final String[] args) throws IOException
    {
        final FileChannel receiveFileChannel = Common.createTmpFileChannel();
        final ByteBuffer receiveByteBuffer = receiveFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MTU_LENGTH_DEFAULT);
        final DatagramChannel receiveDatagramChannel = DatagramChannel.open();
        init(receiveDatagramChannel);
        receiveDatagramChannel.bind(new InetSocketAddress(LOCALHOST, 40124));
        receiveDatagramChannel.connect(new InetSocketAddress(LOCALHOST, 40123));

        final FileChannel sendFileChannel = Common.createTmpFileChannel();
        final ByteBuffer sendByteBuffer = sendFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MTU_LENGTH_DEFAULT);
        final DatagramChannel sendDatagramChannel = DatagramChannel.open();
        init(sendDatagramChannel);
        sendDatagramChannel.bind(new InetSocketAddress(LOCALHOST, 40125));
        sendDatagramChannel.connect(new InetSocketAddress(LOCALHOST, 40126));

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        final int packetSize = SIZE_OF_LONG * 2;

        while (true)
        {
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
            final long receivedTimestamp = receiveByteBuffer.getLong(SIZE_OF_LONG);

            sendByteBuffer.putLong(0, receivedSequenceNumber);
            sendByteBuffer.putLong(SIZE_OF_LONG, receivedTimestamp);

            final long bytesSent = sendFileChannel.transferTo(0, packetSize, sendDatagramChannel);
            if (packetSize != bytesSent)
            {
                throw new IllegalStateException("Invalid bytes sent " + bytesSent);
            }
        }
    }
}
