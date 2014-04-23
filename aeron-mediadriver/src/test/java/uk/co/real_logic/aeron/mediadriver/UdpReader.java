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
package uk.co.real_logic.aeron.mediadriver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import static org.junit.Assert.assertTrue;

/**
 * .Test class used for reading the udp messages that we send
 */
public class UdpReader implements Runnable
{
    private final String host;
    private final int port;
    private final ByteBuffer readBuffer;
    private final DatagramChannel chan;

    private volatile boolean done;

    public UdpReader(final String host, final int port) throws IOException
    {
        this.host = host;
        this.port = port;
        readBuffer = ByteBuffer.allocateDirect(UnicastSenderTest.BUFFER_SIZE);
        chan = DatagramChannel.open().bind(new InetSocketAddress(host, port));
        assertTrue(chan.isOpen());
        new Thread(this).start();
        done = false;
    }

    public void run()
    {
        try
        {
            SocketAddress read = chan.receive(readBuffer);
            done = read != null;
            synchronized (this)
            {
                notify();
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public ByteBuffer awaitResult() throws InterruptedException
    {
        synchronized (this)
        {
            while (!done)
            {
                wait();
            }
        }
        return readBuffer;
    }
}
